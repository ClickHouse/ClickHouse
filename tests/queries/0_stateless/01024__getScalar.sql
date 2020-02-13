CREATE TABLE foo (key String, macro String MATERIALIZED __getScalar(key)) Engine=Null(); -- { serverError 43 }
