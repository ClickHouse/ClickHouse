#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageValues.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>


#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <Common/randomSeed.h>
#include <pcg_random.hpp>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionRandom.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int LOGICAL_ERROR;
}

MutableColumnPtr createColumnWithRandomData(DataTypePtr type, UInt64 limit)
{
    TypeIndex idx = type->getTypeId();
    MutableColumnPtr column = type->createColumn();

    switch (idx)
    {
        case TypeIndex::Nothing:
            for (UInt64 i = 0; i < limit; ++i)
            {
                column->insertDefault();
            }
            throw Exception("Random Generator not implemented for type 'Nothing'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::UInt8:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<UInt8>(generator()));
                }
            }
            break;
        case TypeIndex::UInt16:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<UInt16>(generator()));
                }
            }
            break;
        case TypeIndex::UInt32:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<UInt32>(generator()));
                }
            }
            break;
        case TypeIndex::UInt64:
            {
                pcg64 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<UInt64>(generator()));
                }
            }
            break;
        case TypeIndex::UInt128:
            throw Exception("Random Generator not implemented for type 'UInt128'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Int8:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<Int8>(generator()));
                }
            }
            break;
        case TypeIndex::Int16:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<Int16>(generator()));
                }
            }
            break;
        case TypeIndex::Int32:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<Int32>(generator()));
                }
            }
            break;
        case TypeIndex::Int64:
            {
                pcg64 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<Int64>(generator()));
                }
            }
            break;
        case TypeIndex::Int128:
            throw Exception("Random Generator not implemented for type '" + String(TypeName<Int128>::get()) + "'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Float32:
            {
                pcg32 generator(randomSeed());
                double d;
                for (UInt64 i = 0; i < limit; ++i)
                {
                    d = std::numeric_limits<float>::max();
                    column->insert( (d / pcg32::max()) * generator() );
                }
            }
            break;
        case TypeIndex::Float64:
            {
                pcg64 generator(randomSeed());
                double d;
                for (UInt64 i = 0; i < limit; ++i)
                {
                    d = std::numeric_limits<float>::max();
                    column->insert( (d / pcg64::max()) * generator() );
                }
            }
            break;
        case TypeIndex::Date:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<Int16>(generator()));
                }
            }
            break;
        case TypeIndex::DateTime:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<UInt32>(generator()));
                }
            }
            break;
        case TypeIndex::DateTime64:
            {
                UInt32 scale;
                if (auto * ptype = typeid_cast<const DataTypeDateTime64 *>(type.get()))
                    scale = ptype->getScale();
                else
                    throw Exception("Static cast to DataTypeDateTime64 failed ", ErrorCodes::BAD_TYPE_OF_FIELD);
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    UInt32 fractional = static_cast<UInt32>(generator()) % intExp10(scale);
                    UInt32 whole = static_cast<UInt32>(generator());
                    DateTime64 dt = DecimalUtils::decimalFromComponents<DateTime64>(whole, fractional, scale);
                    column->insert(DecimalField(dt, scale));
                }
            }
            break;
        case TypeIndex::String:
            throw Exception("Random Generator not implemented for type '" + String(TypeName<String>::get()) + "'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::FixedString:
            throw Exception("Random Generator not implemented for type 'FixedString'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Enum8:
            throw Exception("Random Generator not implemented for type 'Enum8'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Enum16:
            throw Exception("Random Generator not implemented for type 'Enum16'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Decimal32:
            {
                pcg32 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<Int32>(generator()));
                }
            }
            break;
        case TypeIndex::Decimal64:
            {
                pcg64 generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(static_cast<UInt64>(generator()));
                }
            }
            break;
        case TypeIndex::Decimal128:
            throw Exception("Random Generator not implemented for type 'Decimal128'.", ErrorCodes::NOT_IMPLEMENTED);
/*
            {
                UInt32 scale = 0;
                if (auto * ptype = typeid_cast<const DataTypeDecimalBase<Decimal128> *>(type.get()))
                    scale = ptype->getScale();
                else
                    throw Exception("Static cast to Decimal128 failed ", ErrorCodes::BAD_TYPE_OF_FIELD);

                pcg128_once_insecure generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i)
                {
                    column->insert(DecimalField(static_cast<Int128>(generator()), scale));
                }
            }
            break;
*/
        case TypeIndex::UUID:
            {
                pcg128_once_insecure generator(randomSeed());
                for (UInt64 i = 0; i < limit; ++i) {
                    column->insert(static_cast<UInt128>(generator()));
                }
            }
            break;
        case TypeIndex::Array:
            throw Exception("Random Generator not implemented for type 'Array'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Tuple:
            throw Exception("Random Generator not implemented for type 'Tuple'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Set:
            throw Exception("Random Generator not implemented for type 'Set'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Interval:
            throw Exception("Type 'Interval' can not be stored in a table.", ErrorCodes::LOGICAL_ERROR);
        case TypeIndex::Nullable:
            throw Exception("Random Generator not implemented for type 'Nullable'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Function:
            throw Exception("Random Generator not implemented for type 'Function'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::AggregateFunction:
            throw Exception("Random Generator not implemented for type 'AggregateFunction'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::LowCardinality:
            throw Exception("Random Generator not implemented for type 'LowCardinality'.", ErrorCodes::NOT_IMPLEMENTED);
    }
    return column;
}

StoragePtr TableFunctionRandom::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() > 2)
        throw Exception("Table function '" + getName() + "' requires one or two arguments: structure (and limit).",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// Parsing first argument as table structure and creating a sample block
    std::string structure = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    UInt64 limit = 1;
    /// Parsing second argument if present
    if (args.size() == 2)
        limit = args[1]->as<ASTLiteral &>().value.safeGet<UInt64>();

    if (!limit)
        throw Exception("Table function '" + getName() + "' limit should not be 0.", ErrorCodes::BAD_ARGUMENTS);

    ColumnsDescription columns = parseColumnsListFromString(structure, context);

    Block res_block;
    for (const auto & name_type : columns.getOrdinary())
    {
        MutableColumnPtr column = createColumnWithRandomData(name_type.type, limit);
        res_block.insert({std::move(column), name_type.type, name_type.name});
    }

    auto res = StorageValues::create(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

void registerTableFunctionRandom(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionRandom>(TableFunctionFactory::CaseInsensitive);
}

}
