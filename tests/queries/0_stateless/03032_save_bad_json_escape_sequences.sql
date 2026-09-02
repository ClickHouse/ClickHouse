set input_format_json_throw_on_bad_escape_sequence=0;

select * from format(JSONEachRow, $$
{"key" : "\u"}
{"key" : "\ud"}
{"key" : "\ud8"}
{"key" : "\ud80"}
{"key" : "\ud800"}
{"key" : "\ud800\"}
{"key" : "\ud800\u"}
{"key" : "\ud800\u1"}
{"key" : "\ud800\u12"}
{"key" : "\ud800\u123"}
{"key" : "\ud800\u1234"}
$$);

