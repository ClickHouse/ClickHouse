set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';
print '-- isnan --';
print isnan(double(nan));
print isnan(4.2);
print isnan(4); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print isnan(real(+inf));
print isnan(dynamic(null)); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
