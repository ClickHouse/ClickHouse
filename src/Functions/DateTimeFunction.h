
namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

/// Common implementation for makeDateTime, makeDateTime64
class DateTimeFunction
{
protected:
    template <typename T>
    static Int64 dateTime(T year, T month, T day_of_month, T hour, T minute, T second, const DateLUTImpl & lut)
    {
        ///  Note that hour, minute and second are checked against 99 to behave consistently with parsing DateTime from String
        ///  E.g. "select cast('1984-01-01 99:99:99' as DateTime);" returns "1984-01-05 04:40:39"
        if (unlikely(std::isnan(year) || std::isnan(month) || std::isnan(day_of_month) ||
                     std::isnan(hour) || std::isnan(minute) || std::isnan(second) ||
                     year < DATE_LUT_MIN_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31 ||
                     hour < 0 || hour > 99 || minute < 0 || minute > 99 || second < 0 || second > 99))
            return minDateTime(lut);

        if (unlikely(year > DATE_LUT_MAX_YEAR))
            return maxDateTime(lut);

        return lut.makeDateTime(year, month, day_of_month, hour, minute, second);
    }

    static Int64 minDateTime(const DateLUTImpl & lut)
    {
        return lut.makeDateTime(DATE_LUT_MIN_YEAR - 1, 1, 1, 0, 0, 0);
    }

    static Int64 maxDateTime(const DateLUTImpl & lut)
    {
        return lut.makeDateTime(DATE_LUT_MAX_YEAR + 1, 1, 1, 23, 59, 59);
    }

    virtual String getName() const = 0;

    std::string extractTimezone(const ColumnWithTypeAndName & timezone_argument) const
    {
        std::string timezone;
        if (!isStringOrFixedString(timezone_argument.type) || !timezone_argument.column || (timezone_argument.column->size() != 1 && !typeid_cast<const ColumnConst*>(timezone_argument.column.get())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument 'timezone' for function {} must be const string", getName());
        timezone = timezone_argument.column->getDataAt(0).toString();

        return timezone;
    }

    virtual ~DateTimeFunction() = default;
};

}
