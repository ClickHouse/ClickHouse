#include <boost/bind.hpp>
#include <numeric>

#include <Poco/DirectoryIterator.h>
#include <Poco/Ext/ScopedTry.h>

#include <Yandex/time2str.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnNested.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNested.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingFinalBlockInputStream.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/FilterBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTNameTypePair.h>

#include <DB/Interpreters/sortBlock.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>

#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Storages/MergeTree/MergedBlockOutputStream.h>

#include <algorithm>


namespace DB
{

StorageMergeTree::StorageMergeTree(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), columns(columns_),
	context(context_), primary_expr_ast(primary_expr_ast_->clone()),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_), 
	index_granularity(index_granularity_),
	sign_column(sign_column_),
	settings(settings_),
	increment(full_path + "increment.txt"), log(&Logger::get("StorageMergeTree: " + name)),
	file_name_regexp("^(\\d{8})_(\\d{8})_(\\d+)_(\\d+)_(\\d+)")
{
	min_marks_for_seek = (settings.min_rows_for_seek + index_granularity - 1) / index_granularity;
	min_marks_for_concurrent_read = (settings.min_rows_for_concurrent_read + index_granularity - 1) / index_granularity;
	max_marks_to_use_cache = (settings.max_rows_to_use_cache + index_granularity - 1) / index_granularity;

	/// создаём директорию, если её нет
	Poco::File(full_path).createDirectories();

	/// инициализируем описание сортировки
	sort_descr.reserve(primary_expr_ast->children.size());
	for (ASTs::iterator it = primary_expr_ast->children.begin();
		it != primary_expr_ast->children.end();
		++it)
	{
		String name = (*it)->getColumnName();
		sort_descr.push_back(SortColumnDescription(name, 1));
	}

	primary_expr = ExpressionAnalyzer(primary_expr_ast, context, *columns).getActions(false);
	
	ExpressionActionsPtr projected_expr = ExpressionAnalyzer(primary_expr_ast, context, *columns).getActions(true);
	primary_key_sample = projected_expr->getSampleBlock();

	merge_threads = new boost::threadpool::pool(settings.merging_threads);
	
	loadDataParts();
}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
{
	return (new StorageMergeTree(path_, name_, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_, index_granularity_, sign_column_, settings_))->thisPtr();
}


StorageMergeTree::~StorageMergeTree()
{
	joinMergeThreads();
}


BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query)
{
	return new MergeTreeBlockOutputStream(thisPtr());
}


BlockInputStreams StorageMergeTree::read(
	const Names & column_names_to_return,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	check(column_names_to_return);
	processed_stage = QueryProcessingStage::FetchColumns;
	
	PKCondition key_condition(query, context, *columns, sort_descr);
	PKCondition date_condition(query, context, *columns, SortDescription(1, SortColumnDescription(date_column_name, 1)));

	typedef std::vector<DataPartPtr> PartsList;
	PartsList parts;
	
	/// Выберем куски, в которых могут быть данные, удовлетворяющие date_condition.
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		
		for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
			if (date_condition.mayBeTrueInRange(Row(1, static_cast<UInt64>((*it)->left_date)),Row(1, static_cast<UInt64>((*it)->right_date))))
				parts.push_back(*it);
	}
	
	/// Семплирование.
	Names column_names_to_read = column_names_to_return;
	UInt64 sampling_column_value_limit = 0;
	typedef Poco::SharedPtr<ASTFunction> ASTFunctionPtr;
	ASTFunctionPtr filter_function;
	ExpressionActionsPtr filter_expression;

	ASTSelectQuery & select = *dynamic_cast<ASTSelectQuery*>(&*query);
	if (select.sample_size)
	{
		double size = apply_visitor(FieldVisitorConvertToNumber<double>(),
										   dynamic_cast<ASTLiteral&>(*select.sample_size).value);
		if (size < 0)
			throw Exception("Negative sample size", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		if (size > 1)
		{
			size_t requested_count = apply_visitor(FieldVisitorConvertToNumber<UInt64>(), dynamic_cast<ASTLiteral&>(*select.sample_size).value);

			/// Узнаем, сколько строк мы бы прочли без семплирования.
			LOG_DEBUG(log, "Preliminary index scan with condition: " << key_condition.toString());
			size_t total_count = 0;
			for (size_t i = 0; i < parts.size(); ++i)
			{
				DataPartPtr & part = parts[i];
				MarkRanges ranges = MergeTreeBlockInputStream::markRangesFromPkRange(full_path + part->name + '/',
																					part->size,
																					*this,
																					key_condition);
				for (size_t j = 0; j < ranges.size(); ++j)
					total_count += ranges[j].end - ranges[j].begin;
			}
			total_count *= index_granularity;
			
			size = std::min(1., static_cast<double>(requested_count) / total_count);
			
			LOG_DEBUG(log, "Selected relative sample size: " << size);
		}
		
		UInt64 sampling_column_max = 0;
		DataTypePtr type = primary_expr->getSampleBlock().getByName(sampling_expression->getColumnName()).type;
		
		if (type->getName() == "UInt64")
			sampling_column_max = std::numeric_limits<UInt64>::max();
		else if (type->getName() == "UInt32")
			sampling_column_max = std::numeric_limits<UInt32>::max();
		else if (type->getName() == "UInt16")
			sampling_column_max = std::numeric_limits<UInt16>::max();
		else if (type->getName() == "UInt8")
			sampling_column_max = std::numeric_limits<UInt8>::max();
		else
			throw Exception("Invalid sampling column type in storage parameters: " + type->getName() + ". Must be unsigned integer type.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

		/// Добавим условие, чтобы отсечь еще что-нибудь при повторном просмотре индекса.
		sampling_column_value_limit = static_cast<UInt64>(size * sampling_column_max);
		if (!key_condition.addCondition(sampling_expression->getColumnName(),
			Range::RightBounded(sampling_column_value_limit, true)))
			throw Exception("Sampling column not in primary key", ErrorCodes::ILLEGAL_COLUMN);

		/// Выражение для фильтрации: sampling_expression <= sampling_column_value_limit

		ASTPtr filter_function_args = new ASTExpressionList;
		filter_function_args->children.push_back(sampling_expression);
		filter_function_args->children.push_back(new ASTLiteral(StringRange(), sampling_column_value_limit));

		filter_function = new ASTFunction;
		filter_function->name = "lessOrEquals";
		filter_function->arguments = filter_function_args;
		filter_function->children.push_back(filter_function->arguments);

		filter_expression = ExpressionAnalyzer(filter_function, context, *columns).getActions(false);
		
		/// Добавим столбцы, нужные для sampling_expression.
		std::vector<String> add_columns = filter_expression->getRequiredColumns();
		column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
		std::sort(column_names_to_read.begin(), column_names_to_read.end());
		column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
	}

	LOG_DEBUG(log, "Key condition: " << key_condition.toString());
	LOG_DEBUG(log, "Date condition: " << date_condition.toString());
	
	RangesInDataParts parts_with_ranges;
	
	/// Найдем, какой диапазон читать из каждого куска.
	size_t sum_marks = 0;
	size_t sum_ranges = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		DataPartPtr & part = parts[i];
		RangesInDataPart ranges(part);
		ranges.ranges = MergeTreeBlockInputStream::markRangesFromPkRange(full_path + part->name + '/',
		                                                                 part->size,
												                          *this,
		                                                                 key_condition);
		if (!ranges.ranges.empty())
		{
			parts_with_ranges.push_back(ranges);
			
			sum_ranges += ranges.ranges.size();
			for (size_t j = 0; j < ranges.ranges.size(); ++j)
			{
				sum_marks += ranges.ranges[j].end - ranges.ranges[j].begin;
			}
		}
	}
	
	LOG_DEBUG(log, "Selected " << parts.size() << " parts by date, " << parts_with_ranges.size() << " parts by key, "
			  << sum_marks << " marks to read from " << sum_ranges << " ranges");
	
	BlockInputStreams res;
	
	if (select.final)
	{
		/// Добавим столбцы, нужные для вычисления первичного ключа и знака.
		std::vector<String> add_columns = primary_expr->getRequiredColumns();
		column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
		column_names_to_read.push_back(sign_column);
		std::sort(column_names_to_read.begin(), column_names_to_read.end());
		column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
		
		res = spreadMarkRangesAmongThreadsFinal(parts_with_ranges, threads, column_names_to_read, max_block_size, settings.use_uncompressed_cache);
	}
	else
	{
		res = spreadMarkRangesAmongThreads(parts_with_ranges, threads, column_names_to_read, max_block_size, settings.use_uncompressed_cache);
	}
	
	if (select.sample_size)
	{
		for (size_t i = 0; i < res.size(); ++i)
		{
			BlockInputStreamPtr original_stream = res[i];
			BlockInputStreamPtr expression_stream = new ExpressionBlockInputStream(original_stream, filter_expression);
			BlockInputStreamPtr filter_stream = new FilterBlockInputStream(expression_stream, filter_function->getColumnName());
			res[i] = filter_stream;
		}
	}
	
	return res;
}


/// Примерно поровну распределить засечки между потоками.
BlockInputStreams StorageMergeTree::spreadMarkRangesAmongThreads(
	RangesInDataParts parts, size_t threads, const Names & column_names, size_t max_block_size, bool use_uncompressed_cache)
{
	/// На всякий случай перемешаем куски.
	std::random_shuffle(parts.begin(), parts.end());
	
	/// Посчитаем засечки для каждого куска.
	std::vector<size_t> sum_marks_in_parts(parts.size());
	size_t sum_marks = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		/// Пусть отрезки будут перечислены справа налево, чтобы можно было выбрасывать самый левый отрезок с помощью pop_back().
		std::reverse(parts[i].ranges.begin(), parts[i].ranges.end());
		
		sum_marks_in_parts[i] = 0;
		for (size_t j = 0; j < parts[i].ranges.size(); ++j)
		{
			MarkRange & range = parts[i].ranges[j];
			sum_marks_in_parts[i] += range.end - range.begin;
		}
		sum_marks += sum_marks_in_parts[i];
	}

	if (sum_marks > max_marks_to_use_cache)
		use_uncompressed_cache = false;
	
	BlockInputStreams res;
	
	if (sum_marks > 0)
	{
		size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;
		
		for (size_t i = 0; i < threads && !parts.empty(); ++i)
		{
			size_t need_marks = min_marks_per_thread;
			BlockInputStreams streams;
			
			/// Цикл по кускам.
			while (need_marks > 0 && !parts.empty())
			{
				RangesInDataPart & part = parts.back();
				size_t & marks_in_part = sum_marks_in_parts.back();
				
				/// Не будем брать из куска слишком мало строк.
				if (marks_in_part >= min_marks_for_concurrent_read &&
					need_marks < min_marks_for_concurrent_read)
					need_marks = min_marks_for_concurrent_read;
				
				/// Не будем оставлять в куске слишком мало строк.
				if (marks_in_part > need_marks &&
					marks_in_part - need_marks < min_marks_for_concurrent_read)
					need_marks = marks_in_part;
				
				/// Возьмем весь кусок, если он достаточно мал.
				if (marks_in_part <= need_marks)
				{
					/// Восстановим порядок отрезков.
					std::reverse(part.ranges.begin(), part.ranges.end());
					
					streams.push_back(new MergeTreeBlockInputStream(
						full_path + part.data_part->name + '/', max_block_size, column_names, *this,
						part.data_part, part.ranges, thisPtr(), use_uncompressed_cache));
					need_marks -= marks_in_part;
					parts.pop_back();
					sum_marks_in_parts.pop_back();
					continue;
				}
				
				MarkRanges ranges_to_get_from_part;
				
				/// Цикл по отрезкам куска.
				while (need_marks > 0)
				{
					if (part.ranges.empty())
						throw Exception("Unexpected end of ranges while spreading marks among threads", ErrorCodes::LOGICAL_ERROR);
					
					MarkRange & range = part.ranges.back();
					size_t marks_in_range = range.end - range.begin;
				
					size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);
					ranges_to_get_from_part.push_back(MarkRange(range.begin, range.begin + marks_to_get_from_range));
					range.begin += marks_to_get_from_range;
					marks_in_part -= marks_to_get_from_range;
					need_marks -= marks_to_get_from_range;
					if (range.begin == range.end)
						part.ranges.pop_back();
				}
				
				streams.push_back(new MergeTreeBlockInputStream(
					full_path + part.data_part->name + '/', max_block_size, column_names, *this,
					part.data_part, ranges_to_get_from_part, thisPtr(), use_uncompressed_cache));
			}
			
			if (streams.size() == 1)
				res.push_back(streams[0]);
			else
				res.push_back(new ConcatBlockInputStream(streams));
		}
		
		if (!parts.empty())
			throw Exception("Couldn't spread marks among threads", ErrorCodes::LOGICAL_ERROR);
	}
	
	return res;
}


/// Распределить засечки между потоками и сделать, чтобы в ответе (почти) все данные были сколлапсированы (модификатор FINAL).
BlockInputStreams StorageMergeTree::spreadMarkRangesAmongThreadsFinal(
	RangesInDataParts parts, size_t threads, const Names & column_names, size_t max_block_size, bool use_uncompressed_cache)
{
	size_t sum_marks = 0;
	for (size_t i = 0; i < parts.size(); ++i)
		for (size_t j = 0; j < parts[i].ranges.size(); ++j)
			sum_marks += parts[i].ranges[j].end - parts[i].ranges[j].begin;

	if (sum_marks > max_marks_to_use_cache)
		use_uncompressed_cache = false;
	
	ExpressionActionsPtr sign_filter_expression;
	String sign_filter_column;
	createPositiveSignCondition(sign_filter_expression, sign_filter_column);
	
	BlockInputStreams to_collapse;
	
	for (size_t part_index = 0; part_index < parts.size(); ++part_index)
	{
		RangesInDataPart & part = parts[part_index];
		
		BlockInputStreamPtr source_stream = new MergeTreeBlockInputStream(
			full_path + part.data_part->name + '/', max_block_size, column_names, *this,
			part.data_part, part.ranges, thisPtr(), use_uncompressed_cache);
		
		to_collapse.push_back(new ExpressionBlockInputStream(source_stream, primary_expr));
	}
	
	BlockInputStreams res;
	if (to_collapse.size() == 1)
		res.push_back(new FilterBlockInputStream(new ExpressionBlockInputStream(to_collapse[0], sign_filter_expression), sign_filter_column));
	else if (to_collapse.size() > 1)
		res.push_back(new CollapsingFinalBlockInputStream(to_collapse, sort_descr, sign_column));
	
	return res;
}


void StorageMergeTree::createPositiveSignCondition(ExpressionActionsPtr & out_expression, String & out_column)
{
	ASTFunction * function = new ASTFunction;
	ASTPtr function_ptr = function;
	
	ASTExpressionList * arguments = new ASTExpressionList;
	ASTPtr arguments_ptr = arguments;
	
	ASTIdentifier * sign = new ASTIdentifier;
	ASTPtr sign_ptr = sign;
	
	ASTLiteral * one = new ASTLiteral;
	ASTPtr one_ptr = one;
	
	function->name = "equals";
	function->arguments = arguments_ptr;
	function->children.push_back(arguments_ptr);
	
	arguments->children.push_back(sign_ptr);
	arguments->children.push_back(one_ptr);
	
	sign->name = sign_column;
	sign->kind = ASTIdentifier::Column;
	
	one->type = new DataTypeInt8;
	one->value = Field(static_cast<Int64>(1));
	
	out_expression = ExpressionAnalyzer(function_ptr, context, *columns).getActions(false);
	out_column = function->getColumnName();
}


String StorageMergeTree::getPartName(DayNum_t left_date, DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level)
{
	DateLUTSingleton & date_lut = DateLUTSingleton::instance();
	
	/// Имя директории для куска иммет вид: YYYYMMDD_YYYYMMDD_N_N_L.
	String res;
	{
		unsigned left_date_id = Date2OrderedIdentifier(date_lut.fromDayNum(left_date));
		unsigned right_date_id = Date2OrderedIdentifier(date_lut.fromDayNum(right_date));

		WriteBufferFromString wb(res);

		writeIntText(left_date_id, wb);
		writeChar('_', wb);
		writeIntText(right_date_id, wb);
		writeChar('_', wb);
		writeIntText(left_id, wb);
		writeChar('_', wb);
		writeIntText(right_id, wb);
		writeChar('_', wb);
		writeIntText(level, wb);
	}

	return res;
}


void StorageMergeTree::loadDataParts()
{
	LOG_DEBUG(log, "Loading data parts");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		
	DateLUTSingleton & date_lut = DateLUTSingleton::instance();
	data_parts.clear();

	Poco::DirectoryIterator end;
	Poco::RegularExpression::MatchVec matches;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		std::string file_name = it.name();

		if (!isPartDirectory(file_name, matches))
			continue;
			
		DataPartPtr part = new DataPart(*this);
		part->left_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[1].offset, matches[1].length)));
		part->right_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[2].offset, matches[2].length)));
		part->left = parse<UInt64>(file_name.substr(matches[3].offset, matches[3].length));
		part->right = parse<UInt64>(file_name.substr(matches[4].offset, matches[4].length));
		part->level = parse<UInt32>(file_name.substr(matches[5].offset, matches[5].length));
		part->name = file_name;

		/// Размер - в количестве засечек.
		part->size = Poco::File(full_path + file_name + "/" + escapeForFileName(columns->front().first) + ".mrk").getSize()
			/ MERGE_TREE_MARK_SIZE;
			
		part->modification_time = it->getLastModified().epochTime();

		part->left_month = date_lut.toFirstDayNumOfMonth(part->left_date);
		part->right_month = date_lut.toFirstDayNumOfMonth(part->right_date);

		data_parts.insert(part);
	}

	all_data_parts = data_parts;

	/** Удаляем из набора актуальных кусков куски, которые содержатся в другом куске (которые были склеены),
	  *  но по каким-то причинам остались лежать в файловой системе.
	  * Удаление файлов будет произведено потом в методе clearOldParts.
	  */

	if (data_parts.size() >= 2)
	{
		DataParts::iterator prev_jt = data_parts.begin();
		DataParts::iterator curr_jt = prev_jt;
		++curr_jt;
		while (curr_jt != data_parts.end())
		{
			/// Куски данных за разные месяцы рассматривать не будем
			if ((*curr_jt)->left_month != (*curr_jt)->right_month
				|| (*curr_jt)->right_month != (*prev_jt)->left_month
				|| (*prev_jt)->left_month != (*prev_jt)->right_month)
			{
				++prev_jt;
				++curr_jt;
				continue;
			}

			if ((*curr_jt)->contains(**prev_jt))
			{
				LOG_WARNING(log, "Part " << (*curr_jt)->name << " contains " << (*prev_jt)->name);
				data_parts.erase(prev_jt);
				prev_jt = curr_jt;
				++curr_jt;
			}
			else if ((*prev_jt)->contains(**curr_jt))
			{
				LOG_WARNING(log, "Part " << (*prev_jt)->name << " contains " << (*curr_jt)->name);
				data_parts.erase(curr_jt++);
			}
			else
			{
				++prev_jt;
				++curr_jt;
			}
		}
	}

	LOG_DEBUG(log, "Loaded data parts (" << data_parts.size() << " items)");
}


void StorageMergeTree::clearOldParts()
{
	Poco::ScopedTry<Poco::FastMutex> lock;

	/// Если метод уже вызван из другого потока (или если all_data_parts прямо сейчас меняют), то можно ничего не делать.
	if (!lock.lock(&all_data_parts_mutex))
	{
		LOG_TRACE(log, "Already clearing or modifying old parts");
		return;
	}
	
	LOG_TRACE(log, "Clearing old parts");
	for (DataParts::iterator it = all_data_parts.begin(); it != all_data_parts.end();)
	{
		int ref_count = it->referenceCount();
		LOG_TRACE(log, (*it)->name << ": ref_count = " << ref_count);
		if (ref_count == 1)		/// После этого ref_count не может увеличиться.
		{
			LOG_DEBUG(log, "Removing part " << (*it)->name);
			
			(*it)->remove();
			all_data_parts.erase(it++);
		}
		else
			++it;
	}
}


void StorageMergeTree::merge(size_t iterations, bool async)
{
	bool while_can = false;
	if (iterations == 0)
	{
		while_can = true;
		iterations = settings.merging_threads;
	}
	
	for (size_t i = 0; i < iterations; ++i)
		merge_threads->schedule(boost::bind(&StorageMergeTree::mergeThread, this, while_can));
	
	if (!async)
		joinMergeThreads();
}


void StorageMergeTree::mergeThread(bool while_can)
{
	try
	{
		std::vector<DataPartPtr> parts;
		while (selectPartsToMerge(parts, false) ||
			   selectPartsToMerge(parts, true))
		{
			mergeParts(parts);

			/// Удаляем старые куски.
			parts.clear();
			clearOldParts();
			
			if (!while_can)
				break;
		}
	}
	catch (const Exception & e)
	{
		LOG_ERROR(log, "Code: " << e.code() << ". " << e.displayText() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString());
	}
	catch (const Poco::Exception & e)
	{
		LOG_ERROR(log, "Poco::Exception: " << e.code() << ". " << e.displayText());
	}
	catch (const std::exception & e)
	{
		LOG_ERROR(log, "std::exception: " << e.what());
	}
	catch (...)
	{
		LOG_ERROR(log, "Unknown exception");
	}
}


void StorageMergeTree::joinMergeThreads()
{
	LOG_DEBUG(log, "Waiting for merge threads to finish.");
	merge_threads->wait();
}


/// Выбираем отрезок из не более чем max_parts_to_merge_at_once кусков так, чтобы максимальный размер был меньше чем в max_size_ratio_to_merge_parts раз больше суммы остальных.
/// Это обеспечивает в худшем случае время O(n log n) на все слияния, независимо от выбора сливаемых кусков, порядка слияния и добавления.
/// При max_parts_to_merge_at_once >= log(max_rows_to_merge_parts/index_granularity)/log(max_size_ratio_to_merge_parts),
/// несложно доказать, что всегда будет что сливать, пока количество кусков больше
/// log(max_rows_to_merge_parts/index_granularity)/log(max_size_ratio_to_merge_parts)*(количество кусков размером больше max_rows_to_merge_parts).
/// Дальше эвристики.
/// Будем выбирать максимальный по включению подходящий отрезок.
/// Из всех таких выбираем отрезок с минимальным максимумом размера.
/// Из всех таких выбираем отрезок с минимальным минимумом размера.
/// Из всех таких выбираем отрезок с максимальной длиной.
bool StorageMergeTree::selectPartsToMerge(std::vector<DataPartPtr> & parts, bool merge_anything_for_old_months)
{
	LOG_DEBUG(log, "Selecting parts to merge");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	DateLUTSingleton & date_lut = DateLUTSingleton::instance();
	
	size_t min_max = -1U;
	size_t min_min = -1U;
	int max_len = 0;
	DataParts::iterator best_begin;
	bool found = false;
	
	DayNum_t now_day = date_lut.toDayNum(time(0));
	DayNum_t now_month = date_lut.toFirstDayNumOfMonth(now_day);
		
	/// Сколько кусков, начиная с текущего, можно включить в валидный отрезок, начинающийся левее текущего куска.
	/// Нужно для определения максимальности по включению.
	int max_count_from_left = 0;
	
	/// Левый конец отрезка.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const DataPartPtr & first_part = *it;
		
		max_count_from_left = std::max(0, max_count_from_left - 1);
		
		/// Кусок не занят и достаточно мал.
		if (first_part->currently_merging ||
			first_part->size * index_granularity > settings.max_rows_to_merge_parts)
			continue;
		
		/// Кусок в одном месяце.
		if (first_part->left_month != first_part->right_month)
		{
			LOG_WARNING(log, "Part " << first_part->name << " spans more than one month");
			continue;
		}
		
		/// Самый длинный валидный отрезок, начинающийся здесь.
		size_t cur_longest_max = -1U;
		size_t cur_longest_min = -1U;
		int cur_longest_len = 0;
		
		/// Текущий отрезок, не обязательно валидный.
		size_t cur_max = first_part->size;
		size_t cur_min = first_part->size;
		size_t cur_sum = first_part->size;
		int cur_len = 1;
		
		DayNum_t month = first_part->left_month;
		UInt64 cur_id = first_part->right;
		
		/// Этот месяц кончился хотя бы день назад.
		bool is_old_month = now_day - now_month >= 1 && now_month > month;
		
		/// Правый конец отрезка.
		DataParts::iterator jt = it;
		for (++jt; jt != data_parts.end() && cur_len < static_cast<int>(settings.max_parts_to_merge_at_once); ++jt)
		{
			const DataPartPtr & last_part = *jt;
			
			/// Кусок не занят, достаточно мал и в одном правильном месяце.
			if (last_part->currently_merging ||
				last_part->size * index_granularity > settings.max_rows_to_merge_parts ||
				last_part->left_month != last_part->right_month ||
				last_part->left_month != month)
				break;
			
			/// Кусок правее предыдущего.
			if (last_part->left < cur_id)
			{
				LOG_WARNING(log, "Part " << last_part->name << " intersects previous part");
				break;
			}
			
			cur_max = std::max(cur_max, last_part->size);
			cur_min = std::min(cur_min, last_part->size);
			cur_sum += last_part->size;
			++cur_len;
			cur_id = last_part->right;
			
			/// Если отрезок валидный, то он самый длинный валидный, начинающийся тут.
			if (cur_len >= 2 &&
				(static_cast<double>(cur_max) / (cur_sum - cur_max) < settings.max_size_ratio_to_merge_parts ||
				(is_old_month && merge_anything_for_old_months))) /// За старый месяц объединяем что угодно, если разрешено.
			{
				cur_longest_max = cur_max;
				cur_longest_min = cur_min;
				cur_longest_len = cur_len;
			}
		}
		
		/// Это максимальный по включению валидный отрезок.
		if (cur_longest_len > max_count_from_left)
		{
			max_count_from_left = cur_longest_len;
			
			if (!found ||
				std::make_pair(std::make_pair(cur_longest_max, cur_longest_min), -cur_longest_len) <
				std::make_pair(std::make_pair(min_max, min_min), -max_len))
			{
				found = true;
				min_max = cur_longest_max;
				min_min = cur_longest_min;
				max_len = cur_longest_len;
				best_begin = it;
			}
		}
	}
	
	if (found)
	{
		parts.clear();
		
		DataParts::iterator it = best_begin;
		for (int i = 0; i < max_len; ++i)
		{
			parts.push_back(*it);
			parts.back()->currently_merging = true;
			++it;
		}
		
		LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
	}
	else
	{
		LOG_DEBUG(log, "No parts to merge");
	}
	
	return found;
}


/// parts должны быть отсортированы.
void StorageMergeTree::mergeParts(std::vector<DataPartPtr> parts)
{
	LOG_DEBUG(log, "Merging " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name);

	Names all_column_names;
	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
		all_column_names.push_back(it->first);

	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	StorageMergeTree::DataPartPtr new_data_part = new DataPart(*this);
	new_data_part->left_date = std::numeric_limits<UInt16>::max();
	new_data_part->right_date = std::numeric_limits<UInt16>::min();
	new_data_part->left = parts.front()->left;
	new_data_part->right = parts.back()->right;
	new_data_part->level = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		new_data_part->level = std::max(new_data_part->level, parts[i]->level);
		new_data_part->left_date = std::min(new_data_part->left_date, parts[i]->left_date);
		new_data_part->right_date = std::max(new_data_part->right_date, parts[i]->right_date);
	}
	++new_data_part->level;
	new_data_part->name = getPartName(
		new_data_part->left_date, new_data_part->right_date, new_data_part->left, new_data_part->right, new_data_part->level);
	new_data_part->left_month = date_lut.toFirstDayNumOfMonth(new_data_part->left_date);
	new_data_part->right_month = date_lut.toFirstDayNumOfMonth(new_data_part->right_date);

	/** Читаем из всех кусков, сливаем и пишем в новый.
	  * Попутно вычисляем выражение для сортировки.
	  */
	BlockInputStreams src_streams;

	for (size_t i = 0; i < parts.size(); ++i)
	{
		MarkRanges ranges(1, MarkRange(0, parts[i]->size));
		src_streams.push_back(new ExpressionBlockInputStream(new MergeTreeBlockInputStream(
			full_path + parts[i]->name + '/', DEFAULT_MERGE_BLOCK_SIZE, all_column_names, *this, parts[i], ranges, StoragePtr(), false), primary_expr));
	}

	/// Порядок потоков важен: при совпадении ключа элементы идут в порядке номера потока-источника.
	/// В слитом куске строки с одинаковым ключом должны идти в порядке возрастания идентификатора исходного куска, то есть (примерного) возрастания времени вставки.
	BlockInputStreamPtr merged_stream = sign_column.empty()
		? new MergingSortedBlockInputStream(src_streams, sort_descr, DEFAULT_MERGE_BLOCK_SIZE)
		: new CollapsingSortedBlockInputStream(src_streams, sort_descr, sign_column, DEFAULT_MERGE_BLOCK_SIZE);
	
	MergedBlockOutputStreamPtr to = new MergedBlockOutputStream(*this,
		new_data_part->left_date, new_data_part->right_date, new_data_part->left, new_data_part->right, new_data_part->level);

	copyData(*merged_stream, *to);

	new_data_part->size = to->marksCount();
	new_data_part->modification_time = time(0);

	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

		/// Добавляем новый кусок в набор.
		
		for (size_t i = 0; i < parts.size(); ++i)
		{
			if (data_parts.end() == data_parts.find(parts[i]))
				throw Exception("Logical error: cannot find data part " + parts[i]->name + " in list", ErrorCodes::LOGICAL_ERROR);
		}

		data_parts.insert(new_data_part);
		all_data_parts.insert(new_data_part);
		
		for (size_t i = 0; i < parts.size(); ++i)
		{
			data_parts.erase(data_parts.find(parts[i]));
		}
	}

	LOG_TRACE(log, "Merged " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name);
}


void StorageMergeTree::rename(const String & new_path_to_db, const String & new_name)
{
	joinMergeThreads();
	
	std::string new_full_path = new_path_to_db + escapeForFileName(new_name) + '/';
	
	Poco::File(full_path).renameTo(new_full_path);
	
	path = new_path_to_db;
	full_path = new_full_path;
	name = new_name;
	
	increment.setPath(full_path + "increment.txt");
}


void StorageMergeTree::dropImpl()
{
	joinMergeThreads();
	
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	data_parts.clear();
	all_data_parts.clear();

	Poco::File(full_path).remove(true);
}

/// одинаковыми считаются имена, если они совпадают целиком или nameWithoutDot совпадает с частью имени до точки
bool namesEqual(const String & nameWithoutDot, const DB::NameAndTypePair & name_type)
{
	String nameWithDot = nameWithoutDot + ".";
	return (nameWithDot == name_type.first.substr(0, nameWithoutDot.length() + 1) || nameWithoutDot == name_type.first);
}


void StorageMergeTree::removeColumn(String column_name)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	/// Удаляем колонки из листа columns
	bool is_first = true;
	NamesAndTypesList::iterator column_it;
	do
	{
		column_it = std::find_if(columns->begin(), columns->end(), boost::bind(namesEqual, column_name, _1));

		if (column_it == columns->end())
		{
			if (is_first)
				throw DB::Exception("Wrong column name. Cannot find column to drop", DB::ErrorCodes::ILLEGAL_COLUMN);
		}
		else
			columns->erase(column_it);
		is_first = false;
	}
	while (column_it != columns->end());

	/// Регэксп выбирает файлы столбца для удаления
	Poco::RegularExpression re(column_name + "(?:(?:\\.|\\%2E).+){0,1}" +"(?:\\.mrk|\\.bin|\\.size\\d+\\.bin|\\.size\\d+\\.mrk)");
	/// Цикл по всем директориям кусочков
	Poco::RegularExpression::MatchVec matches;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it_dir = Poco::DirectoryIterator(full_path); it_dir != end; ++it_dir)
	{
		std::string dir_name = it_dir.name();

		if (!isPartDirectory(dir_name, matches))
			continue;

		/// Цикл по каждому из файлов в директории кусочков
		String full_dir_name = full_path + dir_name + "/";
		for (Poco::DirectoryIterator it_file(full_dir_name); it_file != end; ++it_file)
		{
			if (re.match(it_file.name()))
			{
				Poco::File file(full_dir_name + it_file.name());
				if (file.exists())
					file.remove();
			}
		}
	}
}


void StorageMergeTree::alter(const ASTAlterQuery::Parameters & params)
{
	if (params.type == ASTAlterQuery::ADD)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

		NamesAndTypesList::iterator insert_it = columns->end();
		if (params.column)
		{
			String column_name = dynamic_cast<const ASTIdentifier &>(*params.column).name;

			/// Пытаемся найти первую с конца колонку с именем column_name или column_name.*
			NamesAndTypesList::reverse_iterator reverse_insert_it = std::find_if(columns->rbegin(), columns->rend(),  boost::bind(namesEqual, column_name, _1) );

			if (reverse_insert_it == columns->rend())
				throw DB::Exception("Wrong column name. Cannot find column to insert after", DB::ErrorCodes::ILLEGAL_COLUMN);
			else
			{
				/// base возвращает итератор уже смещенный на один элемент вправо
				insert_it = reverse_insert_it.base();
			}
		}

		const ASTNameTypePair & ast_name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
		StringRange type_range = ast_name_type.type->range;
		String type_string = String(type_range.first, type_range.second - type_range.first);

		DB::DataTypePtr data_type = context.getDataTypeFactory().get(type_string);
		NameAndTypePair pair(ast_name_type.name, data_type );
		columns->insert(insert_it, pair);

		/// Медленно, так как каждый раз копируется список
		columns = DataTypeNested::expandNestedColumns(*columns);
		return;
	}
	else if (params.type == ASTAlterQuery::DROP)
	{
		String column_name = dynamic_cast<const ASTIdentifier &>(*params.column).name;
		removeColumn(column_name);
	}
	else
		throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);

}


bool StorageMergeTree::isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec & matches) const
{
	return (file_name_regexp.match(dir_name, 0, matches) && 6 == matches.size());
}

}
