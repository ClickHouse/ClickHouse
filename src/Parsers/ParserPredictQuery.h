#pragma once

#include "IParserBase.h"

namespace DB
{

/// PREDICT(MODEL model_name, TABLE table_name)
class ParserPredictQuery : public IParserBase
{
protected:
    const char * getName() const override { return "PREDICT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
