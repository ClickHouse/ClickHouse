#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h>

using namespace DB;

namespace Iceberg
{

DB::ASTPtr getASTFromTransform(const String & transform_name, const String & column_name)
{
    std::shared_ptr<DB::ASTFunction> function = std::make_shared<ASTFunction>();
    function->arguments = std::make_shared<DB::ASTExpressionList>();
    function->children.push_back(function->arguments);

    if (transform_name == "year" || transform_name == "years")
    {
        function->name = "toYearNumSinceEpoch";
    }
    else if (transform_name == "month" || transform_name == "months")
    {
        function->name = "toMonthNumSinceEpoch";
    }
    else if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
    {
        function->name = "toRelativeDayNum";
    }
    else if (transform_name == "hour" || transform_name == "hours")
    {
        function->name = "toRelativeHourNum";
    }
    else if (transform_name == "identity")
    {
        return std::make_shared<ASTIdentifier>(column_name);
    }
    else if (transform_name.starts_with("truncate"))
    {
        function->name = "icebergTruncate";
        auto argument_start = transform_name.find('[');
        auto argument_width = transform_name.length() - 1 - argument_start;
        std::string width = transform_name.substr(argument_start + 1, argument_width);
        size_t truncate_width = DB::parse<size_t>(width);
        function->arguments->children.push_back(std::make_shared<DB::ASTLiteral>(truncate_width));
    }
    else if (transform_name == "void")
    {
        function->name = "tuple";
        return function;
    }
    else
    {
        return nullptr;
    }

    function->arguments->children.push_back(std::make_shared<DB::ASTIdentifier>(column_name));
    return function;
}

}
