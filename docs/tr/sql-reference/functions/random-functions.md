---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 51
toc_title: "S\xF6zde Rasgele Say\u0131lar Olu\u015Fturma"
---

# Sözde Rasgele sayılar üretmek için Fonksiyonlar {#functions-for-generating-pseudo-random-numbers}

Sözde rasgele sayıların kriptografik olmayan jeneratörleri kullanılır.

Tüm işlevler sıfır bağımsız değişkeni veya bir bağımsız değişkeni kabul eder.
Bir argüman geçirilirse, herhangi bir tür olabilir ve değeri hiçbir şey için kullanılmaz.
Bu argümanın tek amacı, aynı işlevin iki farklı örneğinin farklı rasgele sayılarla farklı sütunlar döndürmesi için ortak alt ifade eliminasyonunu önlemektir.

## Güney Afrika parası {#rand}

Tüm uint32 tipi sayılar arasında eşit olarak dağıtılan bir sözde rasgele uint32 numarası döndürür.
Doğrusal bir uyumlu jeneratör kullanır.

## rand64 {#rand64}

Tüm uint64 tipi sayılar arasında eşit olarak dağıtılan sözde rasgele bir uint64 numarası döndürür.
Doğrusal bir uyumlu jeneratör kullanır.

## randConstant {#randconstant}

Bir sözde rastgele uint32 numarası döndürür, değer farklı bloklar için birdir.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
