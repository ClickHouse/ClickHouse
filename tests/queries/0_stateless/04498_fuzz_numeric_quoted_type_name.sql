-- Quoted type names whose unquoted form parses back as a literal (numbers, true/false,
-- inf/nan) cannot survive the formatting round-trip, so they are rejected.
create table t (x `3`) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x DateTime64(`3`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x Nullable(`3`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x `3ab`) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x Nullable(`true`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x Nullable(`false`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x DateTime64(`inf`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x DateTime64(`Infinity`)) engine Memory; -- { clientError SYNTAX_ERROR }
create table t (x DateTime64(`nan`)) engine Memory; -- { clientError SYNTAX_ERROR }
