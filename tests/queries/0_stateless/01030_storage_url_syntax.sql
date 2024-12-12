-- Tags: no-fasttest
-- no-fasttest: Timeout for the first query (CANNOT_DETECT_FORMAT) is too slow: https://github.com/ClickHouse/ClickHouse/issues/67939

drop table if exists test_table_url_syntax
;
create table test_table_url_syntax (id UInt32) ENGINE = URL('')
; -- { serverError UNSUPPORTED_URI_SCHEME }
create table test_table_url_syntax (id UInt32) ENGINE = URL('','','','')
; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
drop table if exists test_table_url_syntax
;

drop table if exists test_table_url
;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint')
; -- { serverError CANNOT_DETECT_FORMAT }

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint.json');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'ErrorFormat')
; -- { serverError UNKNOWN_FORMAT }

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'gzip');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'gz');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'deflate');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'brotli');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'lzma');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'zstd');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'lz4');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'bz2');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'snappy');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'none');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'auto');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint.gz', 'JSONEachRow');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint.fr', 'JSONEachRow');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow');
drop table test_table_url;

create table test_table_url(id UInt32) ENGINE = URL('http://localhost/endpoint', 'JSONEachRow', 'zip')
; -- { serverError NOT_IMPLEMENTED }

