#pragma once

#include <Parsers/IASTFormatter.h>

namespace DB
{

class IASTJSONFormatter : public IASTFormatter
{
public:
    void format(const IAST & ast, WriteBuffer & buf) override { ast.writeJSON(buf, 0); }

    String getName() const override { return "json"; }
};

}
