#include <cstddef>
#include <string>
#include <vector>
#include <type_traits>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Protocol.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Session.h>
#include <Interpreters/loadMetadata.h>
#include <fmt/format.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/embed.h>
#include <pybind11/stl.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>
#include <Client/LocalConnection.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/typeid_cast.h>
#include "base/types.h"
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/registerStorages.h>
#include <Storages/Cache/registerRemoteFileMetadatas.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <fmt/format.h>
#include <Formats/registerFormats.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;
namespace py = pybind11;

DB::ConfigurationPtr setupUsers();

static DatabasePtr createMemoryDatabaseIfNotExists(ContextPtr context, const String & database_name)
{
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>(database_name, context);
        DatabaseCatalog::instance().attachDatabase(database_name, system_database);
    }
    return system_database;
}


class ClickhouseInstanceHolder
{

public:
    ClickhouseInstanceHolder()
    {
        holder.context->setUsersConfig(setupUsers());

        DatabaseCatalog::instance().attachDatabase("default", std::make_shared<DatabaseMemory>("default", holder.context));
        holder.context->setCurrentDatabase("default");

        attachSystemTablesLocal(holder.context, *createMemoryDatabaseIfNotExists(holder.context, DatabaseCatalog::SYSTEM_DATABASE));
        attachInformationSchema(holder.context, *createMemoryDatabaseIfNotExists(holder.context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(holder.context, *createMemoryDatabaseIfNotExists(holder.context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));

        DB::registerTableFunctions();
        DB::registerAggregateFunctions();
        DB::registerFunctions();
        DB::registerStorages();
        DB::registerDictionaries();
        DB::registerDisks();
        DB::registerFormats();
        DB::registerRemoteFileMetadatas();
    }

    ContextHolder holder;

};

ContextMutablePtr getNewSessionContext()
{
    static ClickhouseInstanceHolder holder;
    ContextMutablePtr new_session_context;
    new_session_context = Context::createCopy(holder.holder.context);
    new_session_context->makeQueryContext();
    new_session_context->setCurrentQueryId("");

    return new_session_context;
}


static ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}


DB::ConfigurationPtr setupUsers()
{
    static const char * minimal_default_user_xml =
        "<clickhouse>"
        "    <profiles>"
        "        <default></default>"
        "    </profiles>"
        "    <users>"
        "        <default>"
        "            <password></password>"
        "            <networks>"
        "                <ip>::/0</ip>"
        "            </networks>"
        "            <profile>default</profile>"
        "            <quota>default</quota>"
        "        </default>"
        "    </users>"
        "    <quotas>"
        "        <default></default>"
        "    </quotas>"
        "</clickhouse>";

    return getConfigurationFromXMLString(minimal_default_user_xml);
}

template <typename T>
bool tryConvert(ColumnPtr column, py::dict & result_dict, const char * out_name)
{
    py::list out;
    if constexpr (std::is_same_v<T, std::string>)
    {
        auto col = typeid_cast<const ColumnString *>(column.get());
        if (!col)
            return false;

        if (!result_dict.contains(out_name))
            result_dict[out_name] = out;

        for (size_t i = 0; i < col->size(); ++i)
            result_dict[out_name].cast<py::list>().append(col->getDataAt(i).data);

        return true;
    }
    if constexpr (std::is_integral<T>() || std::is_floating_point<T>())
    {
        auto col = typeid_cast<const ColumnVector<T> *>(column.get());
        if (!col)
            return false;

        const auto & col_data = col->getData();

        if (!result_dict.contains(out_name))
            result_dict[out_name] = out;

        py::buffer_info old_buff = result_dict[out_name].cast<py::array_t<T>>().request();
        size_t old_size = old_buff.size;

        py::array_t<T> tmp(old_size + col->size());
        py::buffer_info new_buff = tmp.request();

        T *ptr_new = static_cast<T *>(new_buff.ptr);
        T *ptr_old = static_cast<T *>(old_buff.ptr);

        for (size_t i = 0; i < old_size; ++i)
            ptr_new[i] = ptr_old[i];

        for (size_t i = 0; i < col->size(); ++i)
            ptr_new[i + old_size] = col_data[i];

        result_dict[out_name] = tmp;
        return true;
    }
    return false;
}

py::dict query(const std::string & query)
{
    auto io_block = executeQuery(query, getNewSessionContext());

    py::dict result;
    if (io_block.pipeline.pulling())
    {
        PullingPipelineExecutor executor(io_block.pipeline);
        Block block;

        while (executor.pull(block))
        {
            for (const auto & col : block)
            {
                bool convert_success = tryConvert<String>(col.column, result, col.name.data())
                || tryConvert<UInt8>(col.column, result, col.name.data())
                || tryConvert<Int8>(col.column, result, col.name.data())
                || tryConvert<UInt16>(col.column, result, col.name.data())
                || tryConvert<Int16>(col.column, result, col.name.data())
                || tryConvert<UInt32>(col.column, result, col.name.data())
                || tryConvert<Int32>(col.column, result, col.name.data())
                || tryConvert<UInt64>(col.column, result, col.name.data())
                || tryConvert<Int64>(col.column, result, col.name.data())
                || tryConvert<Float32>(col.column, result, col.name.data())
                || tryConvert<Float64>(col.column, result, col.name.data());

                if (!convert_success)
                {
                    std::cout << "unknow type\n";
                    return result;
                }
            }
        }
    }
    else if (io_block.pipeline.completed())
    {
        CompletedPipelineExecutor executor(io_block.pipeline);
        executor.execute();
    }
    return result;
}

template <typename T>
bool tryConvertListToColumn(const py::list & list, Block & input, const std::string & col_name)
{
    if constexpr (std::is_same_v<T, std::string>)
    {
        if (list.empty() || !py::isinstance<py::str>(list[0]))
            return false;

        auto column = DB::ColumnString::create();

        for (auto elem : list)
            column.get()->insertData(elem.cast<T>().c_str(), elem.cast<std::string>().size());

        DataTypePtr type = std::make_shared<DataTypeString>();
        input.insert(ColumnWithTypeAndName(std::move(column), type, col_name));

        return true;
    }
    if constexpr (std::is_integral<T>() || std::is_floating_point<T>())
    {
        bool is_int = std::is_integral<T>() && py::isinstance<py::int_>(list[0]);
        bool is_float = std::is_floating_point<T>() && py::isinstance<py::float_>(list[0]);

        if (!is_int && !is_float)
            return false;

        auto column = DB::ColumnVector<T>::create();
        for (auto elem : list)
            column->getData().push_back(elem.cast<T>());

        DataTypePtr type = std::make_shared<DataTypeNumber<T>>();
        input.insert(ColumnWithTypeAndName(std::move(column), type, col_name));

        return true;
    }
    return false;
}

void create_table(const std::string & query)
{
    executeQuery("CREATE TABLE " + query + " ENGINE = Memory", getNewSessionContext());
}

void insert(const std::string & table_name, const py::dict & data)
{
    std::string query = "INSERT INTO " + table_name + " FORMAT Native";
    auto io_block = executeQuery(query, getNewSessionContext());

    if (io_block.pipeline.pushing())
    {
        PushingPipelineExecutor executor(io_block.pipeline);
        executor.start();
        Block input;
        for (auto item: data) {
            py::list list = item.second.cast<py::list>();

            if (list.empty())
                continue;

            bool convert_success = tryConvertListToColumn<String>(list, input, py::str(item.first))
                || tryConvertListToColumn<Int32>(list, input, py::str(item.first))
                || tryConvertListToColumn<Int64>(list, input, py::str(item.first))
                || tryConvertListToColumn<Float64>(list, input, py::str(item.first));

            if (!convert_success)
            {
                std::cout << "Unknow type\n";
                input = {};
                break;
            }

        }
        if (input)
        {
            executor.push(input);
        }
        executor.finish();

    }
}

PYBIND11_MODULE(clickhousepy, m)
{
    m.doc() = "pybind11 clickhouse python plugin";
    m.def("query", &query, "execute SELECT query");
    m.def("create_table", &create_table, "creates a table");
    m.def("insert", &insert, "create a table if it does not exist, or move data to a table");
}
