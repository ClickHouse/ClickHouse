#include <boost/bind.hpp>
#include <numeric>
#include <sys/statvfs.h>

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
#include <DB/DataStreams/SummingSortedBlockInputStream.h>
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

size_t StorageMergeTree::total_size_of_currently_merging_parts = 0;

StorageMergeTree::StorageMergeTree(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	Mode mode_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), columns(columns_),
	context(context_), primary_expr_ast(primary_expr_ast_->clone()),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_), 
	index_granularity(index_granularity_),
	mode(mode_), sign_column(sign_column_),
	settings(settings_),
	increment(full_path + "increment.txt"), log(&Logger::get("StorageMergeTree: " + name)), shutdown_called(false),
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

	UInt64 max_part_id = 0;
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		max_part_id = std::max(max_part_id, (*it)->right);
	}
	increment.fixIfBroken(max_part_id);
}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	Mode mode_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
{
	return (new StorageMergeTree(
		path_, name_, columns_, context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, mode_, sign_column_, settings_))->thisPtr();
}


void StorageMergeTree::shutdown()
{
	if (shutdown_called)
		return;
	shutdown_called = true;

	joinMergeThreads();
}


StorageMergeTree::~StorageMergeTree()
{
	shutdown();
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
		{
			Field left = static_cast<UInt64>((*it)->left_date);
			Field right = static_cast<UInt64>((*it)->right_date);

			if (date_condition.mayBeTrueInRange(&left, &right))
				parts.push_back(*it);
		}
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
				MarkRanges ranges = MergeTreeBlockInputStream::markRangesFromPkRange(part->index, *this, key_condition);
				
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
			Range::createRightBounded(sampling_column_value_limit, true)))
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

	/// PREWHERE
	ExpressionActionsPtr prewhere_actions;
	String prewhere_column;
	if (select.prewhere_expression)
	{
		ExpressionAnalyzer analyzer(select.prewhere_expression, context, *columns);
		prewhere_actions = analyzer.getActions(false);
		prewhere_column = select.prewhere_expression->getColumnName();
	}
	
	RangesInDataParts parts_with_ranges;
	
	/// Найдем, какой диапазон читать из каждого куска.
	size_t sum_marks = 0;
	size_t sum_ranges = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		DataPartPtr & part = parts[i];
		RangesInDataPart ranges(part);
		ranges.ranges = MergeTreeBlockInputStream::markRangesFromPkRange(part->index, *this, key_condition);
		
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
		
		res = spreadMarkRangesAmongThreadsFinal(
			parts_with_ranges,
			threads,
			column_names_to_read,
			max_block_size,
			settings.use_uncompressed_cache,
			prewhere_actions,
			prewhere_column);
	}
	else
	{
		res = spreadMarkRangesAmongThreads(
			parts_with_ranges,
			threads,
			column_names_to_read,
			max_block_size,
			settings.use_uncompressed_cache,
			prewhere_actions,
			prewhere_column);
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
	RangesInDataParts parts,
	size_t threads,
	const Names & column_names,
	size_t max_block_size,
	bool use_uncompressed_cache,
	ExpressionActionsPtr prewhere_actions,
	const String & prewhere_column)
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
						part.data_part, part.ranges, thisPtr(), use_uncompressed_cache,
						prewhere_actions, prewhere_column));
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
					part.data_part, ranges_to_get_from_part, thisPtr(), use_uncompressed_cache,
					prewhere_actions, prewhere_column));
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
	RangesInDataParts parts,
	size_t threads,
	const Names & column_names,
	size_t max_block_size,
	bool use_uncompressed_cache,
	ExpressionActionsPtr prewhere_actions,
	const String & prewhere_column)
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
			part.data_part, part.ranges, thisPtr(), use_uncompressed_cache,
			prewhere_actions, prewhere_column);
		
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


void StorageMergeTree::parsePartName(const String & file_name, const Poco::RegularExpression::MatchVec & matches, DataPart & part)
{
	DateLUTSingleton & date_lut = DateLUTSingleton::instance();
	
	part.left_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[1].offset, matches[1].length)));
	part.right_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[2].offset, matches[2].length)));
	part.left = parse<UInt64>(file_name.substr(matches[3].offset, matches[3].length));
	part.right = parse<UInt64>(file_name.substr(matches[4].offset, matches[4].length));
	part.level = parse<UInt32>(file_name.substr(matches[5].offset, matches[5].length));
	
	part.left_month = date_lut.toFirstDayNumOfMonth(part.left_date);
	part.right_month = date_lut.toFirstDayNumOfMonth(part.right_date);
}


void StorageMergeTree::loadDataParts()
{
	LOG_DEBUG(log, "Loading data parts");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		
	data_parts.clear();

	Strings part_file_names;
	Strings old_file_names;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		String file_name = it.name();
		
		/// Удаляем временные директории старше суток.
		if (0 == file_name.compare(0, strlen("tmp_"), "tmp_"))
		{
			Poco::File tmp_dir(full_path + file_name);
			
			if (tmp_dir.isDirectory() && tmp_dir.getLastModified().epochTime() + 86400 < time(0))
			{
				LOG_WARNING(log, "Removing temporary directory " << full_path << file_name);
				Poco::File(full_path + file_name).remove(true);
			}
			
			continue;
		}
		
		if (0 == file_name.compare(0, strlen("old_"), "old_"))
			old_file_names.push_back(file_name);
		else
			part_file_names.push_back(file_name);
	}
	
	Poco::RegularExpression::MatchVec matches;
	while (!part_file_names.empty())
	{
		String file_name = part_file_names.back();
		part_file_names.pop_back();
		
		if (!isPartDirectory(file_name, matches))
			continue;

		/// Для битых кусков, которые могут образовываться после грубого перезапуска сервера, восстановить куски, из которых они сделаны.
		if (isBrokenPart(full_path + file_name))
		{
			Strings new_parts = tryRestorePart(full_path, file_name, old_file_names);
			part_file_names.insert(part_file_names.begin(), new_parts.begin(), new_parts.end());
			continue;
		}
		
		DataPartPtr part = new DataPart(*this);
		parsePartName(file_name, matches, *part);
		
		part->name = file_name;

		/// Размер - в количестве засечек.
		part->size = Poco::File(full_path + file_name + "/" + escapeForFileName(columns->front().first) + ".mrk").getSize()
			/ MERGE_TREE_MARK_SIZE;
			
		part->modification_time = Poco::File(full_path + file_name).getLastModified().epochTime();

		try
		{
			part->loadIndex();
		}
		catch (...)
		{
			/// Не будем вставлять в набор кусок с битым индексом. Пропустим кусок и позволим серверу запуститься.
			tryLogCurrentException(__PRETTY_FUNCTION__);
			continue;
		}

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
		if (ref_count == 1)		/// После этого ref_count не может увеличиться.
		{
			LOG_DEBUG(log, "'Removing' part " << (*it)->name << " (prepending old_ to its name)");
			
			(*it)->renameToOld();
			all_data_parts.erase(it++);
		}
		else
			++it;
	}
	
	/// Удалим старые old_ куски.
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		if (0 != it.name().compare(0, strlen("old_"), "old_"))
			continue;
		if (it->isDirectory() && it->getLastModified().epochTime() + settings.old_parts_lifetime < time(0))
		{
			it->remove(true);
		}
	}
}


void StorageMergeTree::merge(size_t iterations, bool async, bool aggressive)
{
	bool while_can = false;
	if (iterations == 0)
	{
		while_can = true;
		iterations = settings.merging_threads;
	}
	
	for (size_t i = 0; i < iterations; ++i)
		merge_threads->schedule(boost::bind(&StorageMergeTree::mergeThread, this, while_can, aggressive));
	
	if (!async)
		joinMergeThreads();
}


void StorageMergeTree::mergeThread(bool while_can, bool aggressive)
{
	try
	{
		while (!shutdown_called)
		{
			{
				/// К концу этого логического блока должен быть вызван деструктор, чтобы затем корректно определить удаленные куски
				Poco::SharedPtr<CurrentlyMergingPartsTagger> what;

				if (!selectPartsToMerge(what, false, aggressive) && !selectPartsToMerge(what, true, aggressive))
					break;

				mergeParts(what);
			}

			if (shutdown_called)
				break;

			/// Удаляем старые куски.
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
/// Дополнительно:
/// 1) с 1:00 до 5:00 ограничение сверху на размер куска в основном потоке увеличивается в несколько раз
/// 2) в зависимоти от возраста кусков меняется допустимая неравномерность при слиянии
/// 3) Молодые куски крупного размера (примерно больше 1 Гб) можно сливать не меньше чем по три
/// 4) Если в одном из потоков идет мердж крупных кусков, то во втором сливать только маленькие кусочки
/// 5) С ростом логарифма суммарного размера кусочков в мердже увеличиваем требование сбалансированности

bool StorageMergeTree::selectPartsToMerge(Poco::SharedPtr<CurrentlyMergingPartsTagger> &what, bool merge_anything_for_old_months, bool aggressive)
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
	int now_hour = date_lut.toHourInaccurate(time(0));

	size_t maybe_used_bytes = total_size_of_currently_merging_parts;
	size_t total_free_bytes = 0;
	struct statvfs fs;

	/// Смотрим количество свободного места в файловой системе
	if (statvfs(full_path.c_str(), &fs) != 0)
		throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

	total_free_bytes = fs.f_bfree * fs.f_bsize;

	/// Сколько кусков, начиная с текущего, можно включить в валидный отрезок, начинающийся левее текущего куска.
	/// Нужно для определения максимальности по включению.
	int max_count_from_left = 0;

	size_t cur_max_rows_to_merge_parts = settings.max_rows_to_merge_parts;

	/// Если ночь, можем мерджить сильно большие куски
	if (now_hour >= 1 && now_hour <= 5)
		cur_max_rows_to_merge_parts *= settings.merge_parts_at_night_inc;

	/// Если есть активный мердж крупных кусков, то ограничаемся мерджем только маленьких частей.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		if ((*it)->currently_merging && (*it)->size * index_granularity > 25 * 1024 * 1024)
		{
			cur_max_rows_to_merge_parts = settings.max_rows_to_merge_parts_second;
			break;
		}
	}

	/// Левый конец отрезка.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const DataPartPtr & first_part = *it;

		max_count_from_left = std::max(0, max_count_from_left - 1);
		
		/// Кусок не занят.
		if (first_part->currently_merging)
			continue;

		/// Кусок достаточно мал или слияние "агрессивное".
		if (first_part->size * index_granularity > cur_max_rows_to_merge_parts
			&& !aggressive)
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
		size_t cur_total_size = first_part->size_in_bytes;
		int cur_len = 1;
		
		DayNum_t month = first_part->left_month;
		UInt64 cur_id = first_part->right;
		
		/// Этот месяц кончился хотя бы день назад.
		bool is_old_month = now_day - now_month >= 1 && now_month > month;
		
		time_t oldest_modification_time = first_part->modification_time;

		/// Правый конец отрезка.
		DataParts::iterator jt = it;
		for (++jt; jt != data_parts.end() && cur_len < static_cast<int>(settings.max_parts_to_merge_at_once); ++jt)
		{
			const DataPartPtr & last_part = *jt;
			
			/// Кусок не занят и в одном правильном месяце.
			if (last_part->currently_merging ||
				last_part->left_month != last_part->right_month ||
				last_part->left_month != month)
				break;

			/// Кусок достаточно мал или слияние "агрессивное".
			if (last_part->size * index_granularity > cur_max_rows_to_merge_parts
				&& !aggressive)
				break;
			
			/// Кусок правее предыдущего.
			if (last_part->left < cur_id)
			{
				LOG_WARNING(log, "Part " << last_part->name << " intersects previous part");
				break;
			}
			
			oldest_modification_time = std::max(oldest_modification_time, last_part->modification_time);
			cur_max = std::max(cur_max, last_part->size);
			cur_min = std::min(cur_min, last_part->size);
			cur_sum += last_part->size;
			cur_total_size += last_part->size_in_bytes;
			++cur_len;
			cur_id = last_part->right;

			int min_len = 2;
			int cur_age_in_sec = time(0) - oldest_modification_time;

			/// Если куски примерно больше 1 Gb и образовались меньше 6 часов назад, то мерджить не меньше чем по 3.
			if (cur_max * index_granularity * 150 > 1024*1024*1024 && cur_age_in_sec < 6*3600)
				min_len = 3;
			
			/// Равен 0.5 если возраст порядка 0, равен 5 если возраст около месяца.
			double time_ratio_modifier = 0.5 + 9 * static_cast<double>(cur_age_in_sec) / (3600*24*30 + cur_age_in_sec);

			/// Двоичный логарифм суммарного размера кусочков
			double log_cur_sum = std::log(cur_sum * index_granularity) / std::log(2);
			/// Равен ~2 если куски маленькие, уменьшается до 0.5 с увеличением суммарного размера до 2^25.
			double size_ratio_modifier = std::max(0.25, 2 - 3 * (log_cur_sum) / (25 + log_cur_sum));

			/// Объединяем все в одну константу
			double ratio = std::max(0.5, time_ratio_modifier * size_ratio_modifier * settings.max_size_ratio_to_merge_parts);

			/// Если отрезок валидный, то он самый длинный валидный, начинающийся тут.
			if (cur_len >= min_len
				&& (static_cast<double>(cur_max) / (cur_sum - cur_max) < ratio
					/// За старый месяц объединяем что угодно, если разрешено и если этому хотя бы 15 дней
					|| (is_old_month && merge_anything_for_old_months && cur_age_in_sec > 3600*24*15)
					/// Если слияние "агрессивное", то сливаем что угодно
					|| aggressive))
			{
				/// Достаточно места на диске, чтобы покрыть уже активные и новый мерджи с запасом в 50%
				if (total_free_bytes > (maybe_used_bytes + cur_total_size) * 1.5)
				{
					cur_longest_max = cur_max;
					cur_longest_min = cur_min;
					cur_longest_len = cur_len;
				}
				else
					LOG_WARNING(log, "Won't merge parts from " << first_part->name << " to " << last_part->name
						<< " because not enough free space: " << total_free_bytes << " free, "
						<< maybe_used_bytes << " already involved in merge, "
						<< cur_total_size << " required now (+50% on overhead)");
			}
		}
		
		/// Это максимальный по включению валидный отрезок.
		if (cur_longest_len > max_count_from_left)
		{
			max_count_from_left = cur_longest_len;
			
			if (!found
				|| std::make_pair(std::make_pair(cur_longest_max, cur_longest_min), -cur_longest_len)
					< std::make_pair(std::make_pair(min_max, min_min), -max_len))
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
		std::vector<DataPartPtr> parts;
		
		DataParts::iterator it = best_begin;
		for (int i = 0; i < max_len; ++i)
		{
			parts.push_back(*it);
			++it;
		}
		what = new CurrentlyMergingPartsTagger(parts, data_parts_mutex);
		
		LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
	}
	else
	{
		LOG_DEBUG(log, "No parts to merge");
	}
	
	return found;
}


/// parts должны быть отсортированы.
void StorageMergeTree::mergeParts(Poco::SharedPtr<CurrentlyMergingPartsTagger> &what)
{
	const std::vector<DataPartPtr> &parts(what->parts);

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
			full_path + parts[i]->name + '/', DEFAULT_MERGE_BLOCK_SIZE, all_column_names, *this, parts[i], ranges,
			StoragePtr(), false, NULL, ""), primary_expr));
	}

	/// Порядок потоков важен: при совпадении ключа элементы идут в порядке номера потока-источника.
	/// В слитом куске строки с одинаковым ключом должны идти в порядке возрастания идентификатора исходного куска, то есть (примерного) возрастания времени вставки.
	BlockInputStreamPtr merged_stream;

	switch (mode)
	{
		case Ordinary:
			merged_stream = new MergingSortedBlockInputStream(src_streams, sort_descr, DEFAULT_MERGE_BLOCK_SIZE);
			break;

		case Collapsing:
			merged_stream = new CollapsingSortedBlockInputStream(src_streams, sort_descr, sign_column, DEFAULT_MERGE_BLOCK_SIZE);
			break;

		case Summing:
			merged_stream = new SummingSortedBlockInputStream(src_streams, sort_descr, DEFAULT_MERGE_BLOCK_SIZE);
			break;

		default:
			throw Exception("Unknown mode of operation for StorageMergeTree: " + toString(mode), ErrorCodes::LOGICAL_ERROR);
	}

	MergedBlockOutputStreamPtr to = new MergedBlockOutputStream(*this,
		new_data_part->left_date, new_data_part->right_date, new_data_part->left, new_data_part->right, new_data_part->level);

	merged_stream->readPrefix();
	to->writePrefix();

	Block block;
	while (!shutdown_called && (block = merged_stream->read()))
		to->write(block);

	if (shutdown_called)
	{
		LOG_INFO(log, "Shutdown requested while merging parts.");
		return;
	}

	merged_stream->readSuffix();
	to->writeSuffix();

	new_data_part->size = to->marksCount();
	new_data_part->modification_time = time(0);
	new_data_part->loadIndex();	/// NOTE Только что записанный индекс заново считывается с диска. Можно было бы формировать его сразу при записи.

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
	
	/// Кажется тут race condition - в этот момент мердж может запуститься снова.

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

void StorageMergeTree::removeColumnFiles(String column_name)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

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
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		alterColumns(params, columns, context);
	}
	if (params.type == ASTAlterQuery::DROP)
	{
		String column_name = dynamic_cast<const ASTIdentifier &>(*params.column).name;
		removeColumnFiles(column_name);
	}
}


bool StorageMergeTree::isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec & matches) const
{
	return (file_name_regexp.match(dir_name, 0, matches) && 6 == matches.size());
}


bool StorageMergeTree::isBrokenPart(const String & path)
{
	/// Проверяем, что первичный ключ непуст.
	
	Poco::File index_file(path + "/primary.idx");

	if (!index_file.exists() || index_file.getSize() == 0)
	{
		LOG_ERROR(log, "Part " << path << " is broken: primary key is empty.");

		return true;
	}

	/// Проверяем, что все засечки непусты и имеют одинаковый размер.

	ssize_t marks_size = -1;
	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
	{
		Poco::File marks_file(path + "/" + escapeForFileName(it->first) + ".mrk");

		/// при добавлении нового столбца в таблицу файлы .mrk не создаются. Не будем ничего удалять.
		if (!marks_file.exists())
			continue;

		if (marks_size == -1)
		{
			marks_size = marks_file.getSize();

			if (0 == marks_size)
			{
				LOG_ERROR(log, "Part " << path << " is broken: " << marks_file.path() << " is empty.");

				return true;
			}
		}
		else
		{
			if (static_cast<ssize_t>(marks_file.getSize()) != marks_size)
			{
				LOG_ERROR(log, "Part " << path << " is broken: marks have different sizes.");

				return true;
			}
		}
	}

	return false;
}

Strings StorageMergeTree::tryRestorePart(const String & path, const String & file_name, Strings & old_parts)
{
	LOG_ERROR(log, "Restoring all old_ parts covered by " << file_name);
	
	Poco::RegularExpression::MatchVec matches;
	Strings restored_parts;
	
	isPartDirectory(file_name, matches);
	DataPart broken_part(*this);
	parsePartName(file_name, matches, broken_part);
	
	for (int i = static_cast<int>(old_parts.size()) - 1; i >= 0; --i)
	{
		DataPart old_part(*this);
		String name = old_parts[i].substr(strlen("old_"));
		if (!isPartDirectory(name, matches))
		{
			LOG_ERROR(log, "Strange file name: " << path + old_parts[i] << "; ignoring");
			old_parts.erase(old_parts.begin() + i);
			continue;
		}
		parsePartName(name, matches, old_part);
		if (broken_part.contains(old_part))
		{
			/// Восстанавливаем все содержащиеся куски. Если некоторые из них содержатся в других, их удалит loadDataParts.
			LOG_ERROR(log, "Restoring part " << path + old_parts[i]);
			Poco::File(path + old_parts[i]).renameTo(path + name);
			old_parts.erase(old_parts.begin() + i);
			restored_parts.push_back(name);
		}
	}
	
	if (restored_parts.size() >= 2)
	{
		LOG_ERROR(log, "Removing broken part " << path + file_name << " because at least 2 old_ parts were restored in its place");
		Poco::File(path + file_name).remove(true);
	}
	else
	{
		LOG_ERROR(log, "Not removing broken part " << path + file_name
			<< " because less than 2 old_ parts were restored in its place. You need to resolve this manually");
	}
	
	return restored_parts;
}

}
