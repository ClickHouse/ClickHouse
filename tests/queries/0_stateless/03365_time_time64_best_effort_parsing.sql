SET enable_time_time64_type = 1, date_time_input_format = 'best_effort';
SELECT ['2010-10-10 23:10:33']::Array(DateTime);
SELECT ['123:10:33']::Array(Time);
SELECT toString(['10:33'])::Array(Time);
SELECT ['123:10:33.123']::Array(Time64);
