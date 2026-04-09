SELECT formatDateTime(toDate('2024-05-07'), '%e/%m/%Y') as _date, length(_date); -- default behavior
SELECT formatDateTime(toDate('2024-05-07'), '%e/%m/%Y') as _date, length(_date) settings formatdatetime_e_with_space_padding = 1;
SELECT formatDateTime(toDate('2024-05-07'), '%e/%m/%Y') as _date, length(_date) settings formatdatetime_e_with_space_padding = 0;
