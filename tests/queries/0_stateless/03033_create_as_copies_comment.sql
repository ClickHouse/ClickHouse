DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.base (a Int32) ENGINE = TinyLog COMMENT 'original comment';
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.copy_without_comment AS base;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.copy_with_comment AS base COMMENT 'new comment';

SELECT comment FROM system.tables WHERE database = {CLICKHOUSE_DATABASE:String} AND name = 'base';
SELECT comment FROM system.tables WHERE database = {CLICKHOUSE_DATABASE:String} AND name = 'copy_without_comment';
SELECT comment FROM system.tables WHERE database = {CLICKHOUSE_DATABASE:String} AND name = 'copy_with_comment';