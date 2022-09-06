set dialect = 'kusto';
print '-- array_length()';
print array_length(dynamic(['John', 'Denver', 'Bob', 'Marley'])) == 4;
print array_length(dynamic([1, 2, 3])) == 3;
print '-- array_sum()';
print array_sum(dynamic([2, 5, 3])) == 10;
print array_sum(dynamic([2.5, 5.5, 3])) == 11;
print '-- array_index_of()';
print array_index_of(dynamic(['John', 'Denver', 'Bob', 'Marley']), 'Marley');
print array_index_of(dynamic([1, 2, 3]), 2);
print '-- array_iif()';
print array_iif(dynamic([true,false,true]), dynamic([1,2,3]), dynamic([4,5,6]));
print array_iif(dynamic([1,0,1]), dynamic([1,2,3]), dynamic([4,5,6]));
print array_iif(dynamic([true,false,true]), dynamic([1,2]), dynamic([4,5,6]));
print array_iif(dynamic(['a','b','c']), dynamic([1,2,3]), dynamic([4,5,6]));
print '-- array_concat()';
print array_concat(dynamic([1,2,3]),dynamic([4,5,6]));
print '-- array_slice()';
print array_slice(dynamic([1,2,3]), 1, 2);
print array_slice(dynamic([1,2,3,4,5]), -3, -2)
print '-- array_split()';
print array_split(dynamic([1,2,3,4,5]), dynamic([1,-2]));
print array_split(dynamic([1,2,3,4,5]), 2);
print array_split(dynamic([1,2,3,4,5]), dynamic([1,3]));
print array_split(dynamic([1,2,3,4,5]), dynamic([-1,-2]));
print '-- array_sort_asc()';
print array_sort_asc(dynamic([1,3,4,5,2]),dynamic(["a","b","c","d","e"]));
print array_sort_asc(split("John,Paul,George,Ringo", ","));
print array_sort_asc(dynamic([null,"blue","yellow","green",null]));
print v=array_sort_asc(dynamic([null,"blue","yellow","green",null]), false);

-- array_sort_desc()