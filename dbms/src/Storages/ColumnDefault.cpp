#include <iomanip>

#include <Storages/ColumnDefault.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>

namespace
{

struct AliasNames
{
    static constexpr const char * DEFAULT = "DEFAULT";
    static constexpr const char * MATERIALIZED = "MATERIALIZED";
    static constexpr const char * ALIAS = "ALIAS";
};

}

namespace DB
{

ColumnDefaultKind columnDefaultKindFromString(const std::string & str)
{
    static const std::unordered_map<std::string, ColumnDefaultKind> map{
        { AliasNames::DEFAULT, ColumnDefaultKind::Default },
        { AliasNames::MATERIALIZED, ColumnDefaultKind::Materialized },
        { AliasNames::ALIAS, ColumnDefaultKind::Alias }
    };

    const auto it = map.find(str);
    return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str};
}


std::string toString(const ColumnDefaultKind kind)
{
    static const std::unordered_map<ColumnDefaultKind, std::string> map{
        { ColumnDefaultKind::Default, AliasNames::DEFAULT },
        { ColumnDefaultKind::Materialized, AliasNames::MATERIALIZED },
        { ColumnDefaultKind::Alias, AliasNames::ALIAS }
    };

    const auto it = map.find(kind);
    return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultKind"};
}


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
    return lhs.kind == rhs.kind && queryToString(lhs.expression) == queryToString(rhs.expression);
}

ColumnDefaults ColumnDefaultsHelper::loadFromContext(const Context & context, const String & database, const String & table)
{
    if (context.getSettingsRef().insert_sample_with_metadata)
    {
        if (!context.isTableExist(database, table))
            return {};

        StoragePtr storage = context.getTable(database, table);
        const ColumnsDescription & table_columns = storage->getColumns();
        return table_columns.defaults;
    }
    return {};
}

void ColumnDefaultsHelper::attach(const ColumnDefaults & column_defaults, Block & sample)
{
    if (column_defaults.empty())
        return;

    for (auto pr : column_defaults)
    {
        std::stringstream ss;
        ss << *pr.second.expression;

        /// Serialize defaults to special columns names.
        /// It looks better to send expression as a column data but sample block has 0 rows.
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeString>();
        col.name = Block::mkSpecialColumnName(toString(pr.second.kind) + ' ' + pr.first + ' ' + ss.str());
        col.column = col.type->createColumnConst(sample.rows(), "");

        sample.insert(std::move(col));
    }
}

ColumnDefaults ColumnDefaultsHelper::extract(Block & sample)
{
    ParserTernaryOperatorExpression parser;
    ColumnDefaults column_defaults;
    std::set<size_t> pos_to_erase;

    for (size_t i = 0; i < sample.columns(); ++i)
    {
        const ColumnWithTypeAndName & column_wtn = sample.safeGetByPosition(i);

        if (Block::isSpecialColumnName(column_wtn.name, AliasNames::DEFAULT) ||
            Block::isSpecialColumnName(column_wtn.name, AliasNames::MATERIALIZED) ||
            Block::isSpecialColumnName(column_wtn.name, AliasNames::ALIAS))
        {
            String str_kind, column_name;
            std::stringstream ss;
            ss << column_wtn.name;
            ss >> str_kind >> column_name;
            size_t expression_pos = str_kind.size() + column_name.size() + 3;
            StringRef expression(&column_wtn.name[expression_pos], column_wtn.name.size() - expression_pos);

            ColumnDefault def;
            def.kind = columnDefaultKindFromString(str_kind);
            def.expression = parseQuery(parser, expression.data, expression.size);

            column_defaults.emplace(column_name, def);
            pos_to_erase.insert(i);
        }
    }

    sample.erase(pos_to_erase);
    return column_defaults;
}

}
