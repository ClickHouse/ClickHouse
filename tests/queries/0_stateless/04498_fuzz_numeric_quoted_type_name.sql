-- Type names starting with a digit cannot survive the formatting round-trip
-- (`3` formats as 3, which parses back as a literal), so they are rejected.
create table t (x `3`) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x DateTime64(`3`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x Nullable(`3`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x `3ab`) engine Memory; -- { clientError SYNTAX_ERROR }
