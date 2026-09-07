set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';
print '-- isnan --';
print isnan(double(nan));
print isnan(4.2);
print isnan(4);  -- used to raise; the rewrite returns the value Kusto gives
print isnan(real(+inf));
print isnan(dynamic(null));  -- used to raise; the rewrite returns the value Kusto gives
