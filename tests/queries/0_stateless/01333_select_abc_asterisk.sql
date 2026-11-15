select *;

--error: should be failed for abc.*;
select abc.*; --{serverError UNKNOWN_IDENTIFIER}
select *, abc.*; --{serverError UNKNOWN_IDENTIFIER}
select abc.*, *; --{serverError UNKNOWN_IDENTIFIER}
