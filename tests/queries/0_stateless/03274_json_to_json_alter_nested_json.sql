SET enable_json_type = 1;
set output_format_native_write_json_as_string = 0;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=8, max_dynamic_types=4)) engine=Memory;
insert into test format JSONAsObject
{"k1" : [{"k1_1" : [{"k1_1_1" : 42}], "k1_2" : [42], "k1_3" : [{"k1_3_1" : 42}]}], "k2" : [42], "k3" : 42, "k4" : 42, "k5" : 42, "k6" : 42, "k7" : 42, "k8" : 42, "k9" : [{"k9_1" : [{"k9_1_1" : 42}], "k9_2" : [42], "k9_3" : [{"k9_3_1" : 42}]}]}
{"k1" : [{"k1_1" : [{"k1_1_1" : 42}], "k1_2" : [[42]], "k1_3" : [{"k1_3_1" : 42}]}], "k2" : [[42]], "k3" : 42, "k4" : 42, "k5" : 42, "k6" : 42, "k7" : 42, "k8" : 42, "k9" : [{"k9_1" : [{"k9_1_1" : 42}], "k9_2" : [[42]], "k9_3" : [{"k9_3_1" : 42}]}]}
{"k1" : [{"k1_1" : [{"k1_1_1" : 42}], "k1_2" : [[[42]]], "k1_3" : [{"k1_3_1" : 42}]}], "k2" : [[[42]]], "k3" : 42, "k4" : 42, "k5" : 42, "k6" : 42, "k7" : 42, "k8" : 42, "k9" : [{"k9_1" : [{"k9_1_1" : 42}], "k9_2" : [[[42]]], "k9_3" : [{"k9_3_1" : 42}]}]}
{"k1" : [{"k1_1" : [{"k1_1_1" : 42}], "k1_2" : [[[[42]]]], "k1_3" : [{"k1_3_1" : 42}]}], "k2" : [[[[42]]]], "k3" : 42, "k4" : 42, "k5" : 42, "k6" : 42, "k7" : 42, "k8" : 42, "k9" : [{"k9_1" : [{"k9_1_1" : 42}], "k9_2" : [[[[42]]]], "k9_3" : [{"k9_3_1" : 42}]}]}
{"k1" : [{"k1_1" : [{"k1_1_1" : 42}], "k1_2" : [{"k1_2_1" : 42}], "k1_3" : [{"k1_3_1" : 42}]}], "k2" : [{"k2_1" : [{"k2_1_1" : 42}], "k2_2" : [{"k2_2_1" : 42}], "k2_3" : [{"k2_3_1" : 42}]}], "k3" : 42, "k4" : 42, "k5" : 42, "k6" : 42, "k7" : 42, "k8" : 42, "k9" : [{"k9_1" : [{"k9_1_1" : 42}], "k9_2" : [{"k9_2_1" : 42}], "k9_3" : [{"k9_3_1" : 42}]}]}


select 'json';
select arrayJoin(distinctJSONPathsAndTypes(json)) from test;
select 'k1';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k1[]))) from test;
select 'k2';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k2[]))) from test;
select 'k9';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k9[]))) from test;
select 'subcolumns';
select json.k1[], json.k1[].k1_1[], json.k1[].k1_1[].k1_1_1, json.k1[].k1_2[], json.k1[].k1_2[].k1_2_1, json.k2[], json.k2[].k2_1[], json.k2[].k2_1[].k2_1_1, json.k2[].k2_2[], json.k2[].k2_2[].k2_2_1, json.k9[], json.k9[].k9_1[], json.k9[].k9_1[].k9_1_1, json.k9[].k9_2[], json.k9[].k9_2[].k9_2_1 from test format JSONColumns;

drop table if exists test2;
create table test2 (json JSON(max_dynamic_paths=16, max_dynamic_types=8)) engine=Memory;
insert into test2 select json from test;
select 'json';
select arrayJoin(distinctJSONPathsAndTypes(json)) from test2;
select 'k1';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k1[]))) from test2;
select 'k2';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k2[]))) from test2;
select 'k9';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k9[]))) from test2;
select 'subcolumns';
select json.k1[], json.k1[].k1_1[], json.k1[].k1_1[].k1_1_1, json.k1[].k1_2[], json.k1[].k1_2[].k1_2_1, json.k2[], json.k2[].k2_1[], json.k2[].k2_1[].k2_1_1, json.k2[].k2_2[], json.k2[].k2_2[].k2_2_1, json.k9[], json.k9[].k9_1[], json.k9[].k9_1[].k9_1_1, json.k9[].k9_2[], json.k9[].k9_2[].k9_2_1 from test format JSONColumns;

create table test3 (json JSON(max_dynamic_paths=4, max_dynamic_types=2)) engine=Memory;
insert into test3 select json from test;
select 'json';
select arrayJoin(distinctJSONPathsAndTypes(json)) from test3;
select 'k1';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k1[]))) from test3;
select 'k2';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k2[]))) from test3;
select 'k9';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k9[]))) from test3;
select 'subcolumns';
select json.k1[], json.k1[].k1_1[], json.k1[].k1_1[].k1_1_1, json.k1[].k1_2[], json.k1[].k1_2[].k1_2_1, json.k2[], json.k2[].k2_1[], json.k2[].k2_1[].k2_1_1, json.k2[].k2_2[], json.k2[].k2_2[].k2_2_1, json.k9[], json.k9[].k9_1[], json.k9[].k9_1[].k9_1_1, json.k9[].k9_2[], json.k9[].k9_2[].k9_2_1 from test format JSONColumns;

create table test4 (json JSON(max_dynamic_paths=8, max_dynamic_types=4)) engine=Memory;
insert into test4 select json from test2;
select 'json';
select arrayJoin(distinctJSONPathsAndTypes(json)) from test4;
select 'k1';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k1[]))) from test4;
select 'k2';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k2[]))) from test4;
select 'k9';
select arrayJoin(distinctJSONPathsAndTypes(arrayJoin(json.k9[]))) from test4;
select 'subcolumns';
select json.k1[], json.k1[].k1_1[], json.k1[].k1_1[].k1_1_1, json.k1[].k1_2[], json.k1[].k1_2[].k1_2_1, json.k2[], json.k2[].k2_1[], json.k2[].k2_1[].k2_1_1, json.k2[].k2_2[], json.k2[].k2_2[].k2_2_1, json.k9[], json.k9[].k9_1[], json.k9[].k9_1[].k9_1_1, json.k9[].k9_2[], json.k9[].k9_2[].k9_2_1 from test format JSONColumns;

drop table test;
drop table test2;
drop table test3;
drop table test4;
