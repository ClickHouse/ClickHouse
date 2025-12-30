#pragma once

#include <IO/WriteBuffer.h>
#include <Parsers/IAST.h>
#include <base/types.h>


namespace DB
{

/** This the base class for all AST exporters you can extend it and
  * export to JSON, and XML, etc...
  */
class IASTFormatter
{
public:
    virtual ~IASTFormatter() = default;
    /// Format the given AST and write the result to the provided WriteBuffer
    virtual void format(const IAST & ast, WriteBuffer & buf) = 0;
    /// Gets the name of the formatter (e.g., "JSON", "XML", etc.)
    virtual String getName() const = 0;
};

using ASTFormatterPtr = std::shared_ptr<IASTFormatter>;

}
