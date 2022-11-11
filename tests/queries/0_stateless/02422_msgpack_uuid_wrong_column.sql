-- Tags: no-parallel, no-fasttest

insert into function file(data_02422.msgpack) select toUUID('f4cdd80d-5d15-4bdc-9527-adcca635ec1f') as uuid settings output_format_msgpack_uuid_representation='ext';
select * from file(data_02422.msgpack, auto, 'x Int32'); -- {serverError ILLEGAL_COLUMN}
