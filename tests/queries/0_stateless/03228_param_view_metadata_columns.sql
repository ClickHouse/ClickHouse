create view paramview as select * from system.numbers where number <= {top:UInt64};

describe paramview(top = 10);

select arrayReduce('sum', (select groupArray(number) from paramview(top=10)));
