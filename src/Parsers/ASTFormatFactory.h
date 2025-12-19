#pragma once

#include <Core/Types.h>
#include <Parsers/ASTJSONFormatter.h>
#include <Parsers/IASTFormatter.h>

namespace DB
{
/// Factory to get AST formatters by name
class ASTFormatFactory
{
public:
    static ASTFormatterPtr create(const String & formatter)
    {
        if (formatter.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No formatter names provided");

        if (formatter == "json")
            return std::make_shared<ASTJSONFormatter>();

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown AST formatter name: {}", formatter);
    }
};

}
