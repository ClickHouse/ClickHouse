-- Tags: no-parallel

select _path from url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');
select _file from url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');
select _file, count() from url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String') group by _file;
select _path, _file, s from url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');
select _path, _file, s from url('http://127.0.0.1:8123/?query=select+1&user=default&password=wrong', LineAsString, 's String'); -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }
