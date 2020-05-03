select round(arrayMannWhitneyUTestTwoSided([3, 1, 4, 15, 25], [2, 4, 1, 3, 1, 1]), 4);
select round(arrayMannWhitneyUTestTwoSided([1, 1, 1, 1, 1, 1, 1], [2, 3, 4, 5, 6]), 4);
select round(arrayMannWhitneyUTestTwoSided([0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6]), 4);

select round(arrayMannWhitneyUTestLess([3, 1, 4, 15, 25], [2, 4, 1, 3, 1, 1]), 4);
select round(arrayMannWhitneyUTestLess([1, 1, 1, 1, 1, 1, 1], [2, 3, 4, 5, 6]), 4);
select round(arrayMannWhitneyUTestLess([0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6]), 4);

select round(arrayMannWhitneyUTestGreater([3, 1, 4, 15, 25], [2, 4, 1, 3, 1, 1]), 4);
select round(arrayMannWhitneyUTestGreater([1, 1, 1, 1, 1, 1, 1], [2, 3, 4, 5, 6]), 4);
select round(arrayMannWhitneyUTestGreater([0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6]), 4);

select round(arrayMannWhitneyUTestTwoSided([1.1, 2, 3, 14.1], [2.1, 3, 5, 25, 31.1]), 4);
select round(arrayMannWhitneyUTestLess([1.1, 2, 3, 14.1], [2.1, 3, 5, 25, 31.1]), 4);
select round(arrayMannWhitneyUTestGreater([1.1, 2, 3, 14.1], [2.1, 3, 5, 25, 31.1]), 4);
