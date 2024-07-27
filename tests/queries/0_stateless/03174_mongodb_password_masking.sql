create or replace table mongodb_uri_password_masking (_id String)
    engine = MongoDB('mongodb://testuser:mypassword@127.0.0.1:27017/example', 'test_clickhouse');
select replaceAll(create_table_query, currentDatabase(), 'default') from system.tables
                          where table = 'mongodb_uri_password_masking' and database = currentDatabase();
select replaceAll(engine_full, currentDatabase(), 'default') from system.tables
                   where table = 'mongodb_uri_password_masking' and database = currentDatabase();
drop table if exists mongodb_uri_password_masking;

create or replace dictionary mongodb_dictionary_uri_password_masking (_id String)
    primary key _id
    source(MONGODB(uri 'mongodb://testuser:mypassword@127.0.0.1:27017/example' collection 'test_clickhouse'))
    layout(FLAT())
    lifetime(0);
select replaceAll(create_table_query, currentDatabase(), 'default') from system.tables
                          where table = 'mongodb_dictionary_uri_password_masking' and database = currentDatabase();
drop dictionary if exists mongodb_dictionary_uri_password_masking;


create table mongodb_password_masking (_id String)
    engine = MongoDB('127.0.0.1:27017', 'example', 'test_clickhouse', 'testuser', 'mypassword');
select replaceAll(create_table_query, currentDatabase(), 'default') from system.tables
                          where table = 'mongodb_password_masking' and database = currentDatabase();
select replaceAll(engine_full, currentDatabase(), 'default') from system.tables
                   where table = 'mongodb_password_masking' and database = currentDatabase();
drop table if exists mongodb_password_masking;

create or replace dictionary mongodb_dictionary_password_masking (_id String)
    primary key _id
    source(MONGODB(
            host '127.0.0.1'
            port 27017
            user 'testuser'
            password 'mypassword'
            db 'example'
            collection 'test_clickhouse'
            options 'ssl=true'
           ))
    layout(FLAT())
    lifetime(0);
select replaceAll(create_table_query, currentDatabase(), 'default') from system.tables
                          where table = 'mongodb_dictionary_password_masking' and database = currentDatabase();
drop dictionary if exists mongodb_dictionary_password_masking;
