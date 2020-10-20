SELECT countEqualRanges(arrayJoin([]));
SELECT countEqualRanges(arrayJoin([1]));
SELECT countEqualRanges(arrayJoin([1,1]));
SELECT countEqualRanges(arrayJoin([1,1,1,2,2,3,3,3,2,2]));
SELECT countEqualRanges(arrayJoin(['a','a','b','b','a','a']));
SELECT countEqualRanges(arrayJoin([(1,'a'),(1,'a'),(1,'a'),(1,'a'),(2,'b'),(2,'c'),(3,'d')]));
SELECT countEqualRanges(arrayJoin([[],[1,2],[1,2],[],[],[]]));
SELECT countEqualRanges(number) from numbers(123456);
