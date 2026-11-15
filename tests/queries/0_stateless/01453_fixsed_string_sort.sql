drop table if exists badFixedStringSort;
CREATE TABLE IF NOT EXISTS badFixedStringSort (uuid5_old FixedString(16), subitem String) engine=MergeTree order by tuple();

INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '1');
INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '2');
INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '1');
INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '2');

INSERT INTO badFixedStringSort values (UUIDStringToNum('8ad8fc5e-a49e-544c-98e6-1140afd79f80'), '2');
INSERT INTO badFixedStringSort values (UUIDStringToNum('8ad8fc5e-a49e-544c-98e6-1140afd79f80'), '1');
INSERT INTO badFixedStringSort values (UUIDStringToNum('8ad8fc5e-a49e-544c-98e6-1140afd79f80'), '2');
INSERT INTO badFixedStringSort values (UUIDStringToNum('8ad8fc5e-a49e-544c-98e6-1140afd79f80'), '1');

INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '1');
INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '2');
INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '1');
INSERT INTO badFixedStringSort values (UUIDStringToNum('999e1140-66ef-5610-9c3a-b3fb33e0fda9'), '2');

optimize table badFixedStringSort final;
select hex(uuid5_old), subitem from badFixedStringSort ORDER BY uuid5_old, subitem;

drop table if exists badFixedStringSort;
