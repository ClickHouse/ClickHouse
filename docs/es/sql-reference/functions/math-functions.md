---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "Matem\xE1tica"
---

# Funciones matemáticas {#mathematical-functions}

Todas las funciones devuelven un número Float64. La precisión del resultado es cercana a la precisión máxima posible, pero el resultado puede no coincidir con el número representable de la máquina más cercano al número real correspondiente.

## e() {#e}

Devuelve un número Float64 que está cerca del número e.

## Ciudad() {#pi}

Returns a Float64 number that is close to the number π.

## exp(x) {#expx}

Acepta un argumento numérico y devuelve un número Float64 cercano al exponente del argumento.

## Información) {#logx-lnx}

Acepta un argumento numérico y devuelve un número Float64 cercano al logaritmo natural del argumento.

## exp2(x) {#exp2x}

Acepta un argumento numérico y devuelve un número Float64 cercano a 2 a la potencia de x.

## log2 (x) {#log2x}

Acepta un argumento numérico y devuelve un número Float64 cercano al logaritmo binario del argumento.

## exp10 (x) {#exp10x}

Acepta un argumento numérico y devuelve un número Float64 cercano a 10 a la potencia de x.

## log10 (x) {#log10x}

Acepta un argumento numérico y devuelve un número Float64 cercano al logaritmo decimal del argumento.

## sqrt(x) {#sqrtx}

Acepta un argumento numérico y devuelve un número Float64 cercano a la raíz cuadrada del argumento.

## Cbrt (x) {#cbrtx}

Acepta un argumento numérico y devuelve un número Float64 cercano a la raíz cúbica del argumento.

## erf(x) {#erfx}

Si ‘x’ no es negativo, entonces `erf(x / σ√2)` es la probabilidad de que una variable aleatoria tenga una distribución normal con desviación estándar ‘σ’ toma el valor que está separado del valor esperado en más de ‘x’.

Ejemplo (regla de tres sigma):

``` sql
SELECT erf(3 / sqrt(2))
```

``` text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc(x) {#erfcx}

Acepta un argumento numérico y devuelve un número Float64 cercano a 1 - erf(x), pero sin pérdida de precisión para grandes ‘x’ valor.

## Lgamma (x) {#lgammax}

El logaritmo de la función gamma.

## ¿Qué puedes encontrar en Neodigit) {#tgammax}

Función gamma.

## sin(x) {#sinx}

Sinusoidal.

## cos(x) {#cosx}

El coseno.

## pantalla) {#tanx}

Tangente.

## (x) {#asinx}

El arco sinusoidal.

## Acerca de) {#acosx}

El arco coseno.

## atan (x) {#atanx}

La tangente del arco.

## pow(x, y), potencia(x, y) {#powx-y-powerx-y}

Toma dos argumentos numéricos x e y. Devuelve un número Float64 cercano a x a la potencia de y.

## IntExp2 {#intexp2}

Acepta un argumento numérico y devuelve un número UInt64 cercano a 2 a la potencia de x.

## IntExp10 {#intexp10}

Acepta un argumento numérico y devuelve un número UInt64 cercano a 10 a la potencia de x.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/math_functions/) <!--hide-->
