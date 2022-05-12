select CAST(assumeNotNull(NULL), 'String')

select toTypeName(assumeNotNull(NULL));
select toColumnName(assumeNotNull(NULL));
select toTypeName(assumeNotNull(materialize(NULL)));
select toColumnName(assumeNotNull(materialize(NULL)));

select toTypeName([assumeNotNull(NULL)]);
select toColumnName([assumeNotNull(NULL)]);
select toTypeName([assumeNotNull(materialize(NULL))]);
select toColumnName([assumeNotNull(materialize(NULL))]);

select toTypeName(map(1, assumeNotNull(NULL)));
select toColumnName(map(1, assumeNotNull(NULL)));
select toTypeName(map(1, assumeNotNull(materialize(NULL))));
select toColumnName(map(1, assumeNotNull(materialize(NULL))));

select toTypeName(tuple(1, assumeNotNull(NULL)));
select toColumnName(tuple(1, assumeNotNull(NULL)));
select toTypeName(tuple(1, assumeNotNull(materialize(NULL))));
select toColumnName(tuple(1, assumeNotNull(materialize(NULL))));

select toTypeName(assumeNotNull(NULL) * 2);
select toColumnName(assumeNotNull(NULL) * 2);
select toTypeName(assumeNotNull(materialize(NULL)) * 2);
select toColumnName(assumeNotNull(materialize(NULL)) * 2);


