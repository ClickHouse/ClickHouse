#pragma once

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/copyData.h>
#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/Storages/StorageMemory.h>
#include <DB/Client/Connection.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/PartHandler.h>
#include <Poco/Net/MessageHeader.h>
#include <statdaemons/HTMLForm.h>


namespace DB
{

/// Базовый класс содержащий основную информацию о внешней таблице и
/// основные функции для извлечения этой информации из текстовых полей.
class BaseExternalTable
{
public:
	std::string file; 		/// Файл с данными или '-' если stdin
	std::string name; 		/// Имя таблицы
	std::string format; 	/// Название формата хранения данных

	/// Описание структуры таблицы: (имя столбца, имя типа данных)
	std::vector<std::pair<std::string, std::string> > structure;

	std::unique_ptr<ReadBuffer> read_buffer;
	Block sample_block;

	virtual ~BaseExternalTable() {};

	/// Инициализировать read_buffer в зависимости от источника данных. По умолчанию не делает ничего.
	virtual void initReadBuffer() {};

	/// Инициализировать sample_block по структуре таблицы сохраненной в structure
	virtual void initSampleBlock(const Context & context)
	{
		for (size_t i = 0; i < structure.size(); ++i)
		{
			ColumnWithNameAndType column;
			column.name = structure[i].first;
			column.type = context.getDataTypeFactory().get(structure[i].second);
			column.column = column.type->createColumn();
			sample_block.insert(column);
		}
	}

	/// Получить данные таблицы - пару (поток с содержимым таблицы, имя таблицы)
	virtual ExternalTableData getData(const Context & context)
	{
		initReadBuffer();
		initSampleBlock(context);
		ExternalTableData res = std::make_pair(new AsynchronousBlockInputStream(context.getFormatFactory().getInput(
			format, *read_buffer, sample_block, DEFAULT_BLOCK_SIZE, context.getDataTypeFactory())), name);
		return res;
	}

protected:
	/// Очистить всю накопленную информацию
	void clean()
	{
		name = "";
		file = "";
		format = "";
		structure.clear();
		sample_block = Block();
		read_buffer.reset();
	}

	/// Функция для отладочного вывода информации
	void write()
	{
		std::cerr << "file " << file << std::endl;
		std::cerr << "name " << name << std::endl;
		std::cerr << "format " << format << std::endl;
		std::cerr << "structure: \n";
		for (size_t i = 0; i < structure.size(); ++i)
			std::cerr << "\t" << structure[i].first << " " << structure[i].second << std::endl;
	}

	static std::vector<std::string> split(const std::string & s, const std::string & d)
	{
		std::vector<std::string> res;
		boost::split(res, s, boost::algorithm::is_any_of(d), boost::algorithm::token_compress_on);
		return res;
	}

	/// Построить вектор structure по текстовому полю structure
	virtual void parseStructureFromStructureField(const std::string & argument)
	{
		std::vector<std::string> vals = split(argument, " ,");

		if (vals.size() & 1)
			throw Exception("Odd number of attributes in section structure", ErrorCodes::BAD_ARGUMENTS);

		for (size_t i = 0; i < vals.size(); i += 2)
			structure.emplace_back(vals[i], vals[i + 1]);
	}

	/// Построить вектор structure по текстовому полю types
	virtual void parseStructureFromTypesField(const std::string & argument)
	{
		std::vector<std::string> vals = split(argument, " ,");

		for (size_t i = 0; i < vals.size(); ++i)
			structure.emplace_back("_" + toString(i + 1), vals[i]);
	}
};


/// Парсинг внешей таблицы, используемый в tcp клиенте.
class ExternalTable : public BaseExternalTable
{
public:
	void initReadBuffer()
	{
		if (file == "-")
			read_buffer.reset(new ReadBufferFromFileDescriptor(STDIN_FILENO));
		else
			read_buffer.reset(new ReadBufferFromFile(file));
	}

	/// Извлечение параметров из variables_map, которая строится по командной строке клиента
	ExternalTable(const boost::program_options::variables_map & external_options)
	{
		if (external_options.count("file"))
			file = external_options["file"].as<std::string>();
		else
			throw Exception("--file field have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);

		if (external_options.count("name"))
			name = external_options["name"].as<std::string>();
		else
			throw Exception("--name field have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);

		if (external_options.count("format"))
			format = external_options["format"].as<std::string>();
		else
			throw Exception("--format field have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);

		if (external_options.count("structure"))
			parseStructureFromStructureField(external_options["structure"].as<std::string>());
		else if (external_options.count("types"))
			parseStructureFromTypesField(external_options["types"].as<std::string>());
		else
			throw Exception("Neither --structure nor --types have not been provided for external table", ErrorCodes::BAD_ARGUMENTS);
	}
};

/// Парсинг внешей таблицы, используемый при отправке таблиц через http
/// Функция handlePart будет вызываться для каждой переданной таблицы,
/// поэтому так же необходимо вызывать clean в конце handlePart.
class ExternalTablesHandler : public Poco::Net::PartHandler, BaseExternalTable
{
public:
	std::vector<std::string> names;

	ExternalTablesHandler(Context & context_, Poco::Net::NameValueCollection params_) : context(context_), params(params_) { }

	void handlePart(const Poco::Net::MessageHeader& header, std::istream& stream)
	{
		/// Буфер инициализируется здесь, а не в виртуальной функции initReadBuffer
		read_buffer.reset(new ReadBufferFromIStream(stream));

		/// Извлекаем коллекцию параметров из MessageHeader
		Poco::Net::NameValueCollection content;
		std::string label;
		Poco::Net::MessageHeader::splitParameters(header.get("Content-Disposition"), label, content);

		/// Получаем параметры
		name = content.get("name", "_data");
		format = params.get(name + "_format", "TabSeparated");

		if (params.has(name + "_structure"))
			parseStructureFromStructureField(params.get(name + "_structure"));
		else if (params.has(name + "_types"))
			parseStructureFromTypesField(params.get(name + "_types"));
		else
			throw Exception("Neither structure nor types have not been provided for external table " + name + ". Use fields " + name + "_structure or " + name + "_types to do so.", ErrorCodes::BAD_ARGUMENTS);

		ExternalTableData data = getData(context);

		/// Создаем таблицу
		NamesAndTypesListPtr columns = new NamesAndTypesList(sample_block.getColumnsList());
		StoragePtr storage = StorageMemory::create(data.second, columns);
		context.addExternalTable(data.second, storage);
		BlockOutputStreamPtr output = storage->write(ASTPtr());

		/// Записываем данные
		data.first->readPrefix();
		output->writePrefix();
		while(Block block = data.first->read())
			output->write(block);
		data.first->readSuffix();
		output->writeSuffix();

		names.push_back(name);
		/// Подготавливаемся к приему следующего файла, для этого очищаем всю полученную информацию
		clean();
	}

private:
	Context & context;
	Poco::Net::NameValueCollection params;
};


}
