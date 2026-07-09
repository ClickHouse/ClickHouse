-- datatable(Supplier:string, Fruit:string, Price: real, Purchase:datetime)
-- [
--     'Aldi','Apple',4,'2016-09-10',
--     'Costco','Apple',2,'2016-09-11',
--     'Aldi','Apple',6,'2016-09-10',
--     'Costco','Snargaluff',100,'2016-09-12',
--     'Aldi','Apple',7,'2016-09-12',
--     'Aldi','Snargaluff',400,'2016-09-11',
--     'Costco','Snargaluff',104,'2016-09-12',
--     'Aldi','Apple',5,'2016-09-12',
--     'Aldi','Snargaluff',600,'2016-09-11',
--     'Costco','Snargaluff',200,'2016-09-10',
-- ]

DROP TABLE IF EXISTS Ledger;
CREATE TABLE Ledger
(    
   Supplier Nullable(String),
   Fruit String ,
   Price Float64,
   Purchase Date 
) ENGINE = Memory;
INSERT INTO Ledger VALUES  ('Aldi','Apple',4,'2016-09-10'), ('Costco','Apple',2,'2016-09-11'), ('Aldi','Apple',6,'2016-09-10'), ('Costco','Snargaluff',100,'2016-09-12'), ('Aldi','Apple',7,'2016-09-12'), ('Aldi','Snargaluff',400,'2016-09-11'),('Costco','Snargaluff',104,'2016-09-12'),('Aldi','Apple',5,'2016-09-12'),('Aldi','Snargaluff',600,'2016-09-11'),('Costco','Snargaluff',200,'2016-09-10');

set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';

print '-- extend #1 --';
Ledger | extend PriceInCents = 100 * Price | take 2;

print '-- extend #2 --';
Ledger | extend PriceInCents = 100 * Price | sort by PriceInCents asc | project Fruit, PriceInCents | take 2;

print '-- extend #3 --';
Ledger | extend PriceInCents = 100 * Price | sort by PriceInCents asc | project Fruit, PriceInCents | summarize AveragePrice = avg(PriceInCents), Purchases = count() by Fruit | extend Sentence = strcat(Fruit, ' cost ', tostring(AveragePrice), ' on average based on ', tostring(Purchases), ' samples.') | project Sentence | sort by Sentence asc;

print '-- extend #4 --';
Ledger | extend a = Price | extend b = a | extend c = a, d = b + 500 | extend Pass = bool(b == a and c == a and d == b + 500) | summarize binary_all_and(Pass);

print '-- extend #5 --';
Ledger | take 2 | extend strcat(Fruit, ' was purchased from ', Supplier, ' for $', tostring(Price), ' on ', tostring(Purchase)) | extend PriceInCents = 100 * Price;

print '-- extend #6 --';
Ledger | extend Price = 100 * Price;

print '-- extend #7 --';
print a = 4 | extend a = 5;

print '-- extend #8 --';
-- print x = 5 | extend array_sort_desc(range(0, x), range(1, x + 1))

print '-- extend #9 --';
print x = 19 | extend = 4 + ; -- { clientError SYNTAX_ERROR }

print '-- extend #10 --';
Ledger | extend PriceInCents = * Price | sort by PriceInCents asc | project Fruit, PriceInCents | summarize AveragePrice = avg(PriceInCents), Purchases = count() by Fruit | extend Sentence = strcat(Fruit, ' cost ', tostring(AveragePrice), ' on average based on ', tostring(Purchases), ' samples.') | project Sentence; -- { clientError SYNTAX_ERROR }

print '-- extend #11 --'; -- should ideally return this in the future: 5	[2,1] because of the alias ex
print x = 5 | extend ex = array_sort_desc(dynamic([1, 2]), dynamic([3, 4]));
