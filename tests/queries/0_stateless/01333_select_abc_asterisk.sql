select *;

-- error: should fail for abc.*;
select abc.*; --{serverError UNKNOWN_IDENTIFIER}
select *, abc.*; --{serverError UNKNOWN_IDENTIFIER}
select abc.*, *; --{serverError UNKNOWN_IDENTIFIER}
