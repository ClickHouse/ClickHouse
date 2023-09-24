create view test_param_view as
with {param_test_val:UInt8} as param_test_val
select param_test_val,
       arrayCount((a)->(a < param_test_val), t.arr) as cnt1
from (select [1,2,3,4,5] as arr) t;

select * from test_param_view(param_test_val = 3);

create view test_param_view2 as
with {param_test_val:UInt8} as param_test_val
select param_test_val,
       arrayCount((a)->(a < param_test_val), t.arr) as cnt1,
       arrayCount((a)->(a < param_test_val+1), t.arr) as cnt2
from (select [1,2,3,4,5] as arr) t;

select * from test_param_view2(param_test_val = 3);