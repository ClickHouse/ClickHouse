#include "protoHelpers.h"
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/TableMetadata.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>
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


    static MutableColumnPtr storeTableMeta(const TableMetadata & meta)
    {
        capnp::MallocMessageBuilder message;
        Proto::Context::Builder proto_context = message.initRoot<Proto::Context>();

        auto proto_databases = proto_context.initDatabases(1);
        auto proto_db = proto_databases[0];
        proto_db.setName(meta.database);

        auto proto_db_tables = proto_db.initTables(1);
        auto proto_table = proto_db_tables[0];
        proto_table.setName(meta.table);

        auto proto_columns = proto_table.initColumns(meta.column_defaults.size());

        size_t column_no = 0;
        for (const auto & pr_column : meta.column_defaults)
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

        return serializeProto(message);
    }


    static void loadTableMeta(const char * data, size_t data_size, TableMetadata & table_meta)
    {
        Proto::Context::Reader proto_context = deserializeProto<Proto::Context>(data, data_size);

        ParserTernaryOperatorExpression parser;

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

                    if (expression_kind == ColumnDefaultKind::Default)
                    {
                        ASTPtr ast = parseQuery(parser, expression, expression.size());
                        table_meta.column_defaults.emplace(column_name, ColumnDefault{expression_kind, ast});
                    }
                }
            }
        }
    }


    static constexpr const char * tableMetaColumnName()
    {
        return "tableMeta";
    }


    Block storeTableMetadata(const TableMetadata & table_meta)
    {
        ColumnWithTypeAndName proto_column;
        proto_column.name = tableMetaColumnName();
        proto_column.type = std::make_shared<DataTypeUInt8>();
        proto_column.column = std::move(storeTableMeta(table_meta));

        Block block;
        block.insert(std::move(proto_column));
        return block;
    }


    void loadTableMetadata(const Block & block, TableMetadata & table_meta)
    {
        /// select metadata type by column name
        if (block.has(tableMetaColumnName()))
        {
            const ColumnWithTypeAndName & column = block.getByName(tableMetaColumnName());
            loadTableMeta(column.column->getDataAt(0).data, column.column->byteSize(), table_meta);
        }
    }
}
