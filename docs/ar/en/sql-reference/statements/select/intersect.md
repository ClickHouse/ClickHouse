---
description: 'توثيق لعبارة INTERSECT'
sidebar_label: 'INTERSECT'
slug: /sql-reference/statements/select/intersect
title: 'عبارة INTERSECT'
doc_type: 'reference'
---

تعيد عبارة `INTERSECT` فقط الصفوف الناتجة من كلٍّ من الاستعلام الأول والاستعلام الثاني. ويجب أن تتطابق الاستعلامات من حيث عدد الأعمدة وترتيبها ونوعها. ويمكن أن تتضمن نتيجة `INTERSECT` صفوفًا مكررة.

تُنفَّذ عبارات `INTERSECT` المتعددة من اليسار إلى اليمين إذا لم تُحدَّد أقواس. ويتمتع العامل `INTERSECT` بأولوية أعلى من العبارتين `UNION` و`EXCEPT`.

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```

يمكن أن يكون الشرط أيَّ تعبير وفقًا لمتطلباتك.

<div id="examples">
  ## أمثلة
</div>

فيما يلي مثال بسيط على تقاطع الأعداد من 1 إلى 10 مع الأعداد من 3 إلى 8:

```sql title="Query"
SELECT number FROM numbers(1,10) INTERSECT SELECT number FROM numbers(3,8);
```

```response title="Response"
┌─number─┐
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
└────────┘
```

يكون `INTERSECT` مفيدًا إذا كان لديك جدولان يشتركان في عمود واحد (أو عدة أعمدة). يمكنك أخذ تقاطع نتائج استعلامين، ما دامت النتائج تحتوي على الأعمدة نفسها. على سبيل المثال، لنفترض أن لدينا بضعة ملايين من الصفوف من البيانات التاريخية للعملات المشفرة تتضمن أسعار التداول وحجم التداول:

```sql title="Query"
CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT *
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

SELECT * FROM crypto_prices
WHERE crypto_name = 'Bitcoin'
ORDER BY trade_date DESC
LIMIT 10;
```

```response title="Response"
┌─trade_date─┬─crypto_name─┬──────volume─┬────price─┬───market_cap─┬──change_1_day─┐
│ 2020-11-02 │ Bitcoin     │ 30771456000 │ 13550.49 │ 251119860000 │  -0.013585099 │
│ 2020-11-01 │ Bitcoin     │ 24453857000 │ 13737.11 │ 254569760000 │ -0.0031840964 │
│ 2020-10-31 │ Bitcoin     │ 30306464000 │ 13780.99 │ 255372070000 │   0.017308505 │
│ 2020-10-30 │ Bitcoin     │ 30581486000 │ 13546.52 │ 251018150000 │   0.008084608 │
│ 2020-10-29 │ Bitcoin     │ 56499500000 │ 13437.88 │ 248995320000 │   0.012552661 │
│ 2020-10-28 │ Bitcoin     │ 35867320000 │ 13271.29 │ 245899820000 │   -0.02804481 │
│ 2020-10-27 │ Bitcoin     │ 33749879000 │ 13654.22 │ 252985950000 │    0.04427984 │
│ 2020-10-26 │ Bitcoin     │ 29461459000 │ 13075.25 │ 242251000000 │  0.0033826586 │
│ 2020-10-25 │ Bitcoin     │ 24406921000 │ 13031.17 │ 241425220000 │ -0.0058658565 │
│ 2020-10-24 │ Bitcoin     │ 24542319000 │ 13108.06 │ 242839880000 │   0.013650347 │
└────────────┴─────────────┴─────────────┴──────────┴──────────────┴───────────────┘
```

لنفترض الآن أن لدينا جدولًا باسم `holdings` يتضمن قائمةً بالعملات المشفّرة التي نملكها، إلى جانب عدد الوحدات:

```sql title="Query"
CREATE TABLE holdings
(
    crypto_name String,
    quantity UInt64
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name);

INSERT INTO holdings VALUES
   ('Bitcoin', 1000),
   ('Bitcoin', 200),
   ('Ethereum', 250),
   ('Ethereum', 5000),
   ('DOGEFI', 10);
   ('Bitcoin Diamond', 5000);
```

يمكننا استخدام `INTERSECT` للإجابة عن أسئلة مثل **&quot;ما العملات التي نملكها والتي جرى تداولها بسعر يزيد على 100 دولار؟&quot;**:

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT
SELECT crypto_name FROM crypto_prices
WHERE price > 100
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
│ Ethereum    │
│ Ethereum    │
└─────────────┘
```

هذا يعني أنه في وقتٍ ما، جرى تداول Bitcoin وEthereum فوق 100$، وأن DOGEFI وBitcoin Diamond لم يُتداولا مطلقًا فوق 100$ (على الأقل استنادًا إلى البيانات المتاحة لدينا هنا في هذا المثال).

<div id="intersect-distinct">
  ## INTERSECT DISTINCT
</div>

لاحظ أنه في الاستعلام السابق كان لدينا عدة حيازات من Bitcoin وEthereum جرى تداولها بأكثر من 100 دولار. وقد يكون من الأفضل إزالة الصفوف المكررة (لأنها لا تضيف سوى تكرار لما نعرفه بالفعل). يمكنك إضافة `DISTINCT` إلى `INTERSECT` لإزالة الصفوف المكررة من النتيجة:

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price > 100;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Ethereum    │
└─────────────┘
```

**راجع أيضًا**

* [UNION](/ar/sql-reference/statements/select/union)
* [EXCEPT](/ar/sql-reference/statements/select/except)