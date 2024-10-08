-- Tags: use-vectorscan

DROP DICTIONARY IF EXISTS regexp_dict1;
DROP TABLE IF EXISTS regexp_dictionary_source_table;

CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String),
) ENGINE=TinyLog;

-- test back reference.

INSERT INTO regexp_dictionary_source_table VALUES (1, 0, 'Linux/(\d+[\.\d]*).+tlinux', ['name', 'version'], ['TencentOS', '\1']);
INSERT INTO regexp_dictionary_source_table VALUES (2, 0, '(\d+)/tclwebkit(\d+[\.\d]*)', ['name', 'version', 'comment'], ['Android', '$1', 'test $1 and $2']);
INSERT INTO regexp_dictionary_source_table VALUES (3, 2, '33/tclwebkit', ['version'], ['13']);
INSERT INTO regexp_dictionary_source_table VALUES (4, 2, '3[12]/tclwebkit', ['version'], ['12']);
INSERT INTO regexp_dictionary_source_table VALUES (5, 2, '3[12]/tclwebkit', ['version'], ['11']);
INSERT INTO regexp_dictionary_source_table VALUES (6, 2, '3[12]/tclwebkit', ['version'], ['10']);

create dictionary regexp_dict1
(
    regexp String,
    name String,
    version Nullable(UInt64),
    comment String default 'nothing'
)
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);

select * from dictionary(regexp_dict1);

select dictGet('regexp_dict1', ('name', 'version', 'comment'), 'Linux/101.tlinux');
select dictGet('regexp_dict1', ('name', 'version', 'comment'), '33/tclwebkit11.10x');
select dictGet('regexp_dict1', ('name', 'version', 'comment'), '30/tclwebkit');
select dictGetOrDefault('regexp_dict1', ('name', 'version', 'comment'), '30/tclwebkit', ('', 0, 'default'));

--test column input

DROP table IF EXISTS needle_table;
CREATE TABLE needle_table
(
    key String
)
ENGINE=TinyLog;

INSERT INTO needle_table select concat(toString(number + 30), '/tclwebkit', toString(number)) from system.numbers limit 15;

select * from needle_table;
select dictGet(regexp_dict1, ('name', 'version'), key) from needle_table;

-- test invalid
INSERT INTO regexp_dictionary_source_table VALUES (6, 2, '3[12]/tclwebkit', ['version'], ['10']);
SYSTEM RELOAD dictionary regexp_dict1; -- { serverError INCORRECT_DICTIONARY_DEFINITION  }

truncate table regexp_dictionary_source_table;

INSERT INTO regexp_dictionary_source_table VALUES (6, 2, '3[12]/tclwebkit', ['version'], ['10']);
SYSTEM RELOAD dictionary regexp_dict1; -- { serverError INCORRECT_DICTIONARY_DEFINITION  }

truncate table regexp_dictionary_source_table;

INSERT INTO regexp_dictionary_source_table VALUES (1, 2, 'Linux/(\d+[\.\d]*).+tlinux', ['name', 'version'], ['TencentOS', '\1']);
INSERT INTO regexp_dictionary_source_table VALUES (2, 3, '(\d+)/tclwebkit(\d+[\.\d]*)', ['name', 'version', 'comment'], ['Android', '$1', 'test $1 and $2']);
INSERT INTO regexp_dictionary_source_table VALUES (3, 1, '(\d+)/tclwebkit(\d+[\.\d]*)', ['name', 'version', 'comment'], ['Android', '$1', 'test $1 and $2']);
SYSTEM RELOAD dictionary regexp_dict1; -- { serverError INCORRECT_DICTIONARY_DEFINITION  }

-- test priority
truncate table regexp_dictionary_source_table;
INSERT INTO regexp_dictionary_source_table VALUES (1, 0, '(\d+)/tclwebkit', ['name', 'version'], ['Android', '$1']);
INSERT INTO regexp_dictionary_source_table VALUES (3, 1, '33/tclwebkit', ['name'], ['Android1']); -- child has more priority than parents.
INSERT INTO regexp_dictionary_source_table VALUES (2, 0, '33/tclwebkit', ['version', 'comment'], ['13', 'matched 3']); -- larger id has lower priority than small id.
SYSTEM RELOAD dictionary regexp_dict1;
select dictGet(regexp_dict1, ('name', 'version', 'comment'), '33/tclwebkit');

truncate table regexp_dictionary_source_table;
SYSTEM RELOAD dictionary regexp_dict1; -- { serverError INCORRECT_DICTIONARY_DEFINITION }

select * from dictionary(regexp_dict1);

DROP DICTIONARY IF EXISTS regexp_dict1;
DROP TABLE IF EXISTS regexp_dictionary_source_table;
DROP TABLE IF EXISTS needle_table;
