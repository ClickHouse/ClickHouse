#include <Storages/MergeTree/ProjectionKeyActions.h>

#include <Core/Block.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{
namespace
{
    bool isKeyPossiblyWrappedByFunctionsImpl(
        ASTPtr & node, const Block & key_block, String & out_key_column_name, DataTypePtr & out_key_column_type)
    {
        String name = node->getColumnNameWithoutAlias();
        const auto * it = key_block.findByName(name);
        if (it)
        {
            out_key_column_name = it->name;
            out_key_column_type = it->type;
            node = std::make_shared<ASTIdentifier>(it->name);
            return true;
        }

        if (const auto * func = node->as<ASTFunction>())
        {
            // Projection aggregating keys cannot be inside another aggregate function. It's possible but doesn't make sense.
            if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
                return false;
            auto & args = func->arguments->children;
            for (auto & arg : args)
            {
                if (!isKeyPossiblyWrappedByFunctionsImpl(arg, key_block, out_key_column_name, out_key_column_type))
                    return false;
            }
            return true;
        }
        else if (node->as<ASTLiteral>())
            return true;

        return false;
    }


    bool isKeyPossiblyWrappedByFunctions(
        ASTPtr & node, const Block & key_block, String & out_key_res_column_name, DataTypePtr & out_key_res_column_type)
    {
        String key_column_name;
        DataTypePtr key_column_type;
        if (!isKeyPossiblyWrappedByFunctionsImpl(node, key_block, key_column_name, key_column_type))
            return false;
        out_key_res_column_name = key_column_name;
        out_key_res_column_type = key_column_type;
        return true;
    }
}


bool ProjectionKeyActions::add(ASTPtr & node, const std::string & node_name, Block & key_block)
{
    String out_key_res_column_name;
    DataTypePtr out_key_res_column_type;

    if (isKeyPossiblyWrappedByFunctions(node, key_block, out_key_res_column_name, out_key_res_column_type))
    {
        key_block.erase(out_key_res_column_name);
        func_map[{out_key_res_column_name, out_key_res_column_type}] = node;
        name_map[node_name] = out_key_res_column_name;
        return true;
    }
    return false;
}

}
