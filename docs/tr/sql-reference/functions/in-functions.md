---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "In \u0130\u015Flet theic implementingisinin uygulanmas\u0131"
---

# In operatörünü uygulamak için işlevler {#functions-for-implementing-the-in-operator}

## içinde, notİn, globalİn, globalNotİn {#in-functions}

Bölümüne bakınız [Operatör İNLERDE](../operators/in.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

Birden çok sütun gruplama sağlayan bir işlev.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Tuples normalde bir argüman için Ara değerler olarak kullanılır operatörler, veya lambda fonksiyonlarının resmi parametrelerin bir listesini oluşturmak için. Tuples bir masaya yazılamaz.

## tupleElement (tuple, n), operatör x. N {#tupleelementtuple-n-operator-x-n}

Bir tuple bir sütun alma sağlayan bir işlev.
‘N’ 1'den başlayarak sütun dizinidir. N sabit olmalıdır. ‘N’ bir sabit olması gerekir. ‘N’ tuple boyutundan daha büyük olmayan katı bir pozitif tamsayı olmalıdır.
İşlevi yürütmek için hiçbir maliyet yoktur.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
