#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{
class ArgMax : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "arg_max()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ArgMin : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "arg_min()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Avg : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "avg()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class AvgIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "avgif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BinaryAllAnd : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_all_and()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BinaryAllOr : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_all_or()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BinaryAllXor : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_all_xor()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BuildSchema : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "buildschema()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Count : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "count()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class CountIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "countif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class DCount : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "dcount()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class DCountIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "dcountif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeBag : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_bag()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeBagIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_bag_if()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeList : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_list()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeListIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_list_if()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeListWithNulls : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_list_with_nulls()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeSet : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_set()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MakeSetIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_set_if()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Max : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "max()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MaxIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "maxif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Min : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "min()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MinIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "minif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Percentile : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "percentile()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Percentilew : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "percentilew()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Percentiles : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "percentiles()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class PercentilesArray : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "percentiles_array()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Percentilesw : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "percentilesw()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class PercentileswArray : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "percentilesw_array()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Stdev : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "stdev()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StdevIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "stdevif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Sum : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sum()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SumIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sumif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class TakeAny : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "take_any()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class TakeAnyIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "take_anyif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Variance : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "variance()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class VarianceIf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "varianceif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};


}
