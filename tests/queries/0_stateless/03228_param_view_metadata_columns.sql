create view paramview as select * from system.numbers where number <= {top:UInt64};

describe paramview; -- { serverError UNSUPPORTED_METHOD }

describe paramview(top = 10);

describe paramview(top = 2 + 2);

select arrayReduce('sum', (select groupArray(number) from paramview(top=10)));
