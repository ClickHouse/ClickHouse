CREATE TABLE foo (key String, macro String MATERIALIZED __getScalar(key)) Engine=Null(); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
