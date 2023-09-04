#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>


namespace DB
{

/** ANALYZE query:
 *  ANALYZE [FULL|SAMPLE] TABLE table_name (column_name [, column_name])
 *  SETTINGS (key=value [, key=value]);
 */

class ASTAnalyzeQuery
{

};

}
