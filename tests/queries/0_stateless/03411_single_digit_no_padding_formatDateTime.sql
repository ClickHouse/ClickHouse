-- { echo On }
SELECT formatDateTime(toDate('2024-05-07'), '%e/%m/%Y');

SELECT formatDateTime(toDate('2024-05-7'), '%e') as _date, length(_date) settings formatdatetime_e_format_with_space_padding = 1;
SELECT formatDateTime(toDate('2024-05-07'), '%e') as _date, length(_date) settings formatdatetime_e_format_with_space_padding = 1;

SELECT formatDateTime(toDate('2024-05-7'), '%e') as _date, length(_date) settings formatdatetime_e_format_with_space_padding = 0;
SELECT formatDateTime(toDate('2024-05-07'), '%e') as _date, length(_date) settings formatdatetime_e_format_with_space_padding = 0;


SELECT formatDateTime(parseDateTime(' 1/02/2024', '%e/%m/%Y'), '%e/%m/%Y') settings formatdatetime_e_format_with_space_padding = 1, parsedatetime_e_requires_space_padding = 1;
SELECT formatDateTime(parseDateTime('1/02/2024', '%e/%m/%Y'), '%e/%m/%Y') settings formatdatetime_e_format_with_space_padding = 0, parsedatetime_e_requires_space_padding = 0;
SELECT formatDateTime(parseDateTime(' 1/02/2024', '%e/%m/%Y'), '%e/%m/%Y')settings formatdatetime_e_format_with_space_padding = 0, parsedatetime_e_requires_space_padding = 0;

SELECT formatDateTime(parseDateTime('1/02/2024', '%e/%m/%Y'), '%e/%m/%Y') settings formatdatetime_e_format_with_space_padding = 1, parsedatetime_e_requires_space_padding = 1; -- { serverError CANNOT_PARSE_DATETIME }
