SELECT trunc(materialize(toLowCardinality(0)), materialize(toLowCardinality(0))); -- { serverError ILLEGAL_COLUMN }
