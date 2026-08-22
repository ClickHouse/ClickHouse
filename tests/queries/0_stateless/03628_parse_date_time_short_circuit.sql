select if(number > 10, parseDateTime(materialize(''), '%d-%m-%Y'), '2020-01-01'::DateTime) from numbers(2);

