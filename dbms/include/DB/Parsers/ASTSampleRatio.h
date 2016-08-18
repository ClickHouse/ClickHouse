#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Коэффициент сэмплирования вида 0.1 или 1/10.
  * Важно сохранять его как рациональное число без преобразования в IEEE-754.
  */
class ASTSampleRatio : public IAST
{
public:
	using BigNum = __uint128_t;	/// Должен вмещать в себя результат перемножения двух UInt64.

	struct Rational
	{
		BigNum numerator = 0;
		BigNum denominator = 1;
	};

	Rational ratio;

	ASTSampleRatio() = default;
	ASTSampleRatio(const StringRange range_) : IAST(range_) {}
	ASTSampleRatio(const StringRange range_, Rational & ratio_) : IAST(range_), ratio(ratio_) {}

	String getID() const override { return "SampleRatio_" + toString(ratio); }

	ASTPtr clone() const override { return std::make_shared<ASTSampleRatio>(*this); }

	static String toString(BigNum num);
	static String toString(Rational ratio);

	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		settings.ostr << toString(ratio);
	}
};

}
