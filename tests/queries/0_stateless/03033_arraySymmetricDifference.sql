drop table if exists array_symmetric_difference;
create table if not exists array_symmetric_difference (
    uid UInt8,
    arr_tpl_1 Array(Tuple(Nullable(Int32), Array(String))),
    arr_tpl_2 Array(Tuple(Nullable(Int32), Array(String))),
    arr_lc_str_1 Array(LowCardinality(String)),
    arr_lc_str_2 Array(LowCardinality(String)),
    nst_1         Nested(intt Int32,
                        strr String),
    nst_2         Nested(intt Int32,
                        strr String)
) engine=MergeTree order by uid;

insert into array_symmetric_difference values (1, [(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])], ['a', 'b', 'c'], ['c', 'a', 'b'], [1, 1], ['a', 'a'], [2, 2], ['b', 'b']);
insert into array_symmetric_difference values (2, [(2, ['d', 'e']), (Null, ['f'])], [(3, ['g', Null]), (2, ['d', 'e'])], ['d', 'e', 'f'], ['f', 'd', 'e'], [2, 2], ['d', 'd'], [3, 3], ['e', 'e']);

insert into array_symmetric_difference values (3, [(3, ['g', 'h']), (Null, ['i'])], [(4, ['j', Null]), (3, ['g', 'h'])], ['g', 'h', 'i'], ['i', 'g', 'h'], [3, 3], ['g', 'g'], [4, 4], ['h', 'h']);

insert into array_symmetric_difference values (4, [(4, ['j', 'k']), (Null, ['l'])], [(5, ['m', Null]), (4, ['j', 'k'])], ['j', 'k', 'l'], ['l', 'j', 'k'], [4, 4], ['j', 'j'], [5, 5], ['k', 'k']);

select arraySymmetricDifference(arr_tpl_1, arr_tpl_2) from array_symmetric_difference;
select arraySymmetricDifference(arr_lc_str_1, arr_lc_str_2) from array_symmetric_difference;
select arraySymmetricDifference(nst_1.intt, nst_2.intt) from array_symmetric_difference;
select arraySymmetricDifference(nst_1.intt, nst_2.strr) from array_symmetric_difference; -- { serverError NO_COMMON_TYPE }

select arraySymmetricDifference(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select arraySymmetricDifference(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arraySymmetricDifference([]);
select arraySymmetricDifference([1, 2]);
select arraySymmetricDifference(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arraySymmetricDifference(1, [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arraySymmetricDifference([1, 2], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arraySymmetricDifference(1, [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select toTypeName(arraySymmetricDifference([(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])]));
