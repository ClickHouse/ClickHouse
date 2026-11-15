select * from format(JSONEachRow, '{"item" : "some string"}, {"item":"\\\\ \ud83d"}') settings input_format_allow_errors_num=1;

