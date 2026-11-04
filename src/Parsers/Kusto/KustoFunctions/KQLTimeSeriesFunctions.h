#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{
class SeriesFir : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fir()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesIir : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_iir()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFitLine : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fit_line()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFitLineDynamic : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fit_line_dynamic()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFit2lines : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fit_2lines()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFit2linesDynamic : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fit_2lines_dynamic()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesOutliers : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_outliers()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesPeriodsDetect : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_periods_detect()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesPeriodsValidate : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_periods_validate()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesStatsDynamic : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_stats_dynamic()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesStats : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_stats()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFillBackward : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fill_backward()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFillConst : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fill_const()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFillForward : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fill_forward()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SeriesFillLinear : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "series_fill_linear()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
