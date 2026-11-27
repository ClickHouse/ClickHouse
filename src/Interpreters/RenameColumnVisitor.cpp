#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Whether `name` or one of its dotted prefixes longer than `min_length` is a column of the table.
bool hasColumnPrefix(const ColumnsDescription & columns, const String & name, size_t min_length)
{
    for (size_t pos = name.find('.', min_length + 1); pos != String::npos; pos = name.find('.', pos + 1))
    {
        if (columns.has(name.substr(0, pos)))
            return true;
    }
    return columns.has(name);
}

/// Whether `name` refers to a subcolumn of the column `column_name`: a column named `name` takes precedence over it.
bool isSubcolumnOf(const ColumnsDescription & columns, const String & column_name, const String & name)
{
    const auto * column = columns.tryGet(column_name);
    return column && !columns.has(name) && column->type->hasSubcolumn(std::string_view(name).substr(column_name.size() + 1));
}

}

bool RenameColumnMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    /// A lambda visits its body itself.
    const auto * function = node->as<ASTFunction>();
    return !function || !isASTLambdaFunction(*function);
}

void RenameColumnMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        data.visit(*identifier, ast);
        return;
    }

    auto * function = ast->as<ASTFunction>();
    if (!function || !isASTLambdaFunction(*function))
        return;

    size_t outer_parameters = data.lambda_parameters.size();
    for (const auto & parameter : function->arguments->children[0]->as<ASTFunction &>().arguments->children)
        if (const auto * parameter_identifier = parameter->as<ASTIdentifier>())
            data.lambda_parameters.push_back(parameter_identifier->name());
    RenameColumnVisitor(data).visit(function->arguments->children[1]);
    data.lambda_parameters.resize(outer_parameters);
}

void RenameColumnData::visit(ASTIdentifier & identifier, ASTPtr & ast) const
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (!identifier_column_name)
        return;

    const auto is_lambda_parameter = [&](const String & name)
    {
        return std::ranges::find(lambda_parameters, name) != lambda_parameters.end();
    };
    if (is_lambda_parameter(identifier.name_parts.front()))
        return;

    /// The renamed reference must not be bound to a lambda parameter.
    const auto check_not_captured = [&]
    {
        if (!columns_after)
            return;
        String first_part = rename_to.substr(0, rename_to.find('.'));
        if (is_lambda_parameter(rename_to) || is_lambda_parameter(first_part))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot rename column {} to {}: {} is a lambda parameter where the column is used",
                backQuote(column_name), backQuote(rename_to), backQuote(is_lambda_parameter(rename_to) ? rename_to : first_part));
    };

    if (identifier_column_name == column_name)
    {
        check_not_captured();
        identifier.setShortName(rename_to);
    }
    else if (identifier_column_name->starts_with(column_name + "."))
    {
        /// `column_name.x` (written as a compound identifier, or as one part as the key expressions built from the metadata
        /// of a data lake are written) is renamed if it refers to a subcolumn of `column_name`, not to another column such
        /// as a physical `column_name.x` in `MergeTree` with `share_nested_offsets = 0`, or to a subcolumn of one.
        if (columns_before && !isSubcolumnOf(*columns_before, column_name, *identifier_column_name)
            && hasColumnPrefix(*columns_before, *identifier_column_name, column_name.size()))
            return;

        check_not_captured();

        if (columns_after)
        {
            String renamed_name = rename_to + identifier_column_name->substr(column_name.size());
            if (!isSubcolumnOf(*columns_after, rename_to, renamed_name)
                && hasColumnPrefix(*columns_after, renamed_name, rename_to.size()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot rename column {} to {}: the reference {} would refer to the column {}",
                    backQuote(column_name), backQuote(rename_to), backQuote(*identifier_column_name), backQuote(renamed_name));
        }

        if (identifier.compound())
        {
            /// The column name can span several parts, as the `n.x` of a `Nested` does, or be a part of one, as in `s.x`.z.
            size_t prefix_parts = 0;
            String prefix;
            while (prefix != column_name && !prefix.starts_with(column_name + ".") && prefix_parts < identifier.name_parts.size())
            {
                if (prefix_parts)
                    prefix += '.';
                prefix += identifier.name_parts[prefix_parts];
                ++prefix_parts;
            }
            Strings name_parts;
            if (prefix == column_name)
                boost::split(name_parts, rename_to, boost::is_any_of("."));
            else if (prefix.starts_with(column_name + "."))
                name_parts.push_back(rename_to + prefix.substr(column_name.size()));
            else
                return;
            name_parts.insert(name_parts.end(), identifier.name_parts.begin() + prefix_parts, identifier.name_parts.end());
            auto renamed = make_intrusive<ASTIdentifier>(std::move(name_parts));
            renamed->setAlias(identifier.tryGetAlias());
            ast = std::move(renamed);
        }
        else
        {
            identifier.setShortName(rename_to + identifier_column_name->substr(column_name.size()));
        }
    }
}

}
