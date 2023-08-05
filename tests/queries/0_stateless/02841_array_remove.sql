SELECT length(arrayRemove(x->(x%2==0), [1,2,3,4,5,6,7,8,9,10])) AS x;
SELECT length(arrayRemove(x -> x LIKE '%Hello%', ['Hello', 'abc World', 'Hello Friend!'])) AS res
