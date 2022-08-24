SELECT toStartOfYear(toDate32('1950-02-02')) SETTINGS enable_date32_results = true;
SELECT toStartOfISOYear(toDate32('1950-02-02')) SETTINGS enable_date32_results = true;
SELECT toStartOfQuarter(toDate32('1950-02-02')) SETTINGS enable_date32_results = true;
SELECT toStartOfMonth(toDate32('1950-02-02')) SETTINGS enable_date32_results = true;
SELECT toMonday(toDate32('1950-02-02')) SETTINGS enable_date32_results = true;
SELECT toLastDayOfMonth(toDate32('1950-02-02')) SETTINGS enable_date32_results = true;

SELECT toStartOfYear(toDate32('1950-02-02')) SETTINGS enable_date32_results = false;
SELECT toStartOfISOYear(toDate32('1950-02-02')) SETTINGS enable_date32_results = false;
SELECT toStartOfQuarter(toDate32('1950-02-02')) SETTINGS enable_date32_results = false;
SELECT toStartOfMonth(toDate32('1950-02-02')) SETTINGS enable_date32_results = false;
SELECT toMonday(toDate32('1950-02-02')) SETTINGS enable_date32_results = false;
SELECT toLastDayOfMonth(toDate32('1950-02-02')) SETTINGS enable_date32_results = false;

SELECT toStartOfYear(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = true;
SELECT toStartOfISOYear(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = true;
SELECT toStartOfQuarter(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = true;
SELECT toStartOfMonth(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = true;
SELECT toMonday(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = true;
SELECT toLastDayOfMonth(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = true;

SELECT toStartOfYear(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = false;
SELECT toStartOfISOYear(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = false;
SELECT toStartOfQuarter(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = false;
SELECT toStartOfMonth(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = false;
SELECT toMonday(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = false;
SELECT toLastDayOfMonth(toDateTime64('1950-02-02 10:20:30', 3)) SETTINGS enable_date32_results = false;

SELECT toTypeName(toStartOfYear(toDate32('1950-02-02'))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toStartOfISOYear(toDate32('1950-02-02'))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toStartOfQuarter(toDate32('1950-02-02'))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toStartOfMonth(toDate32('1950-02-02'))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toMonday(toDate32('1950-02-02'))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toLastDayOfMonth(toDate32('1950-02-02'))) SETTINGS enable_date32_results = true;

SELECT toTypeName(toStartOfYear(toDate32('1950-02-02'))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toStartOfISOYear(toDate32('1950-02-02'))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toStartOfQuarter(toDate32('1950-02-02'))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toStartOfMonth(toDate32('1950-02-02'))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toMonday(toDate32('1950-02-02'))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toLastDayOfMonth(toDate32('1950-02-02'))) SETTINGS enable_date32_results = false;

SELECT toTypeName(toStartOfYear(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toStartOfISOYear(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toStartOfQuarter(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toStartOfMonth(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toMonday(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = true;
SELECT toTypeName(toLastDayOfMonth(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = true;

SELECT toTypeName(toStartOfYear(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toStartOfISOYear(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toStartOfQuarter(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toStartOfMonth(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toMonday(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = false;
SELECT toTypeName(toLastDayOfMonth(toDateTime64('1950-02-02 10:20:30', 3))) SETTINGS enable_date32_results = false;
