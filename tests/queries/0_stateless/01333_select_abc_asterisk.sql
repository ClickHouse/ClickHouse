select *;

--error: should be failed for abc.*;
select abc.*; --{serverError 47}
select *, abc.*; --{serverError 47}
select abc.*, *; --{serverError 47}
