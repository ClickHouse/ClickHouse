#include "protoHelpers.h"
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <ServerMessage.capnp.h>

#include <capnp/serialize.h>
#include <sstream>

/// @sa https://capnproto.org/cxx.html

namespace DB
{
    static MutableColumnPtr serializeProto(capnp::MessageBuilder & message)
    {
        MutableColumnPtr data = DataTypeUInt8().createColumn();

        kj::Array<capnp::word> serialized = messageToFlatArray(message);
        kj::ArrayPtr<const char> bytes = serialized.asChars();

        data->reserve(bytes.size());
        for (size_t i = 0 ; i < bytes.size(); ++i)
            data->insertData(&bytes[i], 1);

        return data;
    }

    template <typename T>
    typename T::Reader deserializeProto(const char * data, size_t data_size)
    {
        const capnp::word * ptr = reinterpret_cast<const capnp::word *>(data);
        auto serialized = kj::arrayPtr(ptr, data_size / sizeof(capnp::word));

        capnp::FlatArrayMessageReader reader(serialized);
        return reader.getRoot<T>();
    }

    static ColumnWithTypeAndName storeContext(const String & column_name, Context & context)
    {
        capnp::MallocMessageBuilder message;
        Proto::Context::Builder proto_context = message.initRoot<Proto::Context>();

        Databases dbs = context.getDatabases();
        auto proto_databases = proto_context.initDatabases(dbs.size());

        size_t db_nomber = 0;
        for (auto & pr_db : dbs)
        {
            const String & database_name = pr_db.first;
            if (database_name == "system")
                continue;

            IDatabase & db = *pr_db.second;

            auto proto_db = proto_databases[db_nomber];
            proto_db.setName(database_name);

            std::unordered_map<String, StoragePtr> tables;
            DatabaseIteratorPtr it_tables = db.getIterator(context);
            while (it_tables->isValid())
            {
                tables[it_tables->name()] = it_tables->table();
                it_tables->next();
            }

            auto proto_tables = proto_db.initTables(tables.size());
            size_t table_no = 0;
            for (const auto & pr_table : tables)
            {
                auto current_table = proto_tables[table_no];
                current_table.setName(pr_table.first);

                const ColumnsDescription & columns = pr_table.second->getColumns();
                auto proto_columns = current_table.initColumns(columns.defaults.size());

                size_t column_no = 0;
                for (const auto& pr_column : columns.defaults)
                {
                    const String & column_name = pr_column.first;
                    const ColumnDefault & def = pr_column.second;
                    std::stringstream ss;
                    ss << def.expression;

                    auto current_column = proto_columns[column_no];
                    current_column.setName(column_name);
                    current_column.getDefault().setKind(static_cast<UInt16>(def.kind));
                    current_column.getDefault().setExpression(ss.str());

                    ++column_no;
                }

                ++table_no;
            }

            ++db_nomber;
        }

        ColumnWithTypeAndName proto_column;
        proto_column.name = column_name;
        proto_column.type = std::make_shared<DataTypeUInt8>();
        proto_column.column = std::move(serializeProto(message));
        return proto_column;
    }

    static void loadTableMetaInfo(const ColumnWithTypeAndName & proto_column, TableMetaInfo & table_meta)
    {
        StringRef plain_data = proto_column.column->getDataAt(0);
        size_t data_size = proto_column.column->byteSize();
        Proto::Context::Reader proto_context = deserializeProto<Proto::Context>(plain_data.data, data_size);

        ParserExpressionElement parser;

        for (auto proto_database : proto_context.getDatabases())
        {
            const String & database_name = proto_database.getName().cStr();
            if (database_name != table_meta.database)
                continue;

            for (auto proto_table : proto_database.getTables())
            {
                String table_name = proto_table.getName().cStr();
                if (table_name != table_meta.table)
                    continue;

                for (auto column : proto_table.getColumns())
                {
                    String column_name = column.getName().cStr();
                    String expression = column.getDefault().getExpression().cStr();
                    ColumnDefaultKind expression_kind = static_cast<ColumnDefaultKind>(column.getDefault().getKind());

                    ASTPtr ast = parseQuery(parser, expression, expression.size());
                    table_meta.column_defaults.emplace(column_name, ColumnDefault{expression_kind, ast});
                }
            }
        }
    }

    static constexpr const char * contextColumnName()
    {
        return "context";
    }

    Block storeContextBlock(Context & context)
    {
        Block block;
        block.insert(storeContext(contextColumnName(), context));
        return block;
    }

    void loadTableMetaInfo(const Block & block, TableMetaInfo & table_meta)
    {
        const ColumnWithTypeAndName & column = block.getByName(contextColumnName());
        loadTableMetaInfo(column, table_meta);
    }
}
