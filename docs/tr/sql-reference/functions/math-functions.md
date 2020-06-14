---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Matematiksel
---

# Matematiksel Fonksiyonlar {#mathematical-functions}

Tüm işlevler bir Float64 numarası döndürür. Sonucun doğruluğu mümkün olan en yüksek hassasiyete yakındır, ancak sonuç, ilgili gerçek sayıya en yakın makine temsil edilebilir numarası ile çakışmayabilir.

## e() {#e}

E numarasına yakın bir Float64 numarası döndürür.

## pi sayısı() {#pi}

Returns a Float64 number that is close to the number π.

## exp(x) {#expx}

Sayısal bir bağımsız değişken kabul eder ve bir Float64 sayı argümanın üs yakın döndürür.

## log (x), L (n(x) {#logx-lnx}

Sayısal bir bağımsız değişken kabul eder ve bağımsız değişken doğal logaritma yakın bir Float64 sayı döndürür.

## exp2 (x) {#exp2x}

Sayısal bir bağımsız değişkeni kabul eder ve X gücüne 2'ye yakın bir Float64 numarası döndürür.

## log2 (x) {#log2x}

Sayısal bir bağımsız değişken kabul eder ve değişken ikili logaritma yakın bir Float64 sayı döndürür.

## exp10 (x) {#exp10x}

Sayısal bir bağımsız değişkeni kabul eder ve 10'a yakın Float64 numarasını x gücüne döndürür.

## log10(x) {#log10x}

Sayısal bir bağımsız değişken kabul eder ve bir float64 sayı bağımsız değişken ondalık logaritması yakın döndürür.

## sqrt(x) {#sqrtx}

Sayısal bir bağımsız değişken kabul eder ve bağımsız değişken kareköküne yakın bir Float64 numarası döndürür.

## TCMB (x) {#cbrtx}

Sayısal bir bağımsız değişkeni kabul eder ve bağımsız değişken kübik köküne yakın bir Float64 numarası döndürür.

## erf (x) {#erfx}

Eğer ‘x’ negatif değil, o zaman `erf(x / σ√2)` standart sapma ile normal dağılıma sahip bir rasgele değişkenin olasılığı var mı ‘σ’ beklenen değerden daha fazla ayrılan değeri alır ‘x’.

Örnek (üç sigma kuralı):

``` sql
SELECT erf(3 / sqrt(2))
```

``` text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc (x) {#erfcx}

Sayısal bir bağımsız değişkeni kabul eder ve 1 - erf(x) yakın bir Float64 numarası döndürür, ancak büyük için hassasiyet kaybı olmadan ‘x’ değerler.

## lgamma (x) {#lgammax}

Gama fonksiyonunun logaritması.

## tgamma (x) {#tgammax}

Gama fonksiyonu.

## günah(x) {#sinx}

Sinüs.

## C (os (x) {#cosx}

Kosinüs.

## tan (x) {#tanx}

Teğet.

## asin (x) {#asinx}

Ark sinüsü.

## acos (x) {#acosx}

Ark kosinüsü.

## atan (x) {#atanx}

Ark teğet.

## pow (x, y), güç (x, y)) {#powx-y-powerx-y}

İki sayısal bağımsız değişken X ve y alır.X'e yakın bir Float64 numarasını y gücüne döndürür.

## ıntexp2 {#intexp2}

Sayısal bir bağımsız değişkeni kabul eder ve X'in gücüne 2'ye yakın bir uint64 numarası döndürür.

## ıntexp10 {#intexp10}

Sayısal bir bağımsız değişkeni kabul eder ve X gücüne 10'a yakın bir uint64 numarası döndürür.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/math_functions/) <!--hide-->
