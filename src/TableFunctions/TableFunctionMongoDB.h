#pragma once

#include <Common/Exception.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

std::pair<String, ASTPtr> getKeyValueMongoDBArgument(const ASTFunction * ast_func);

}

