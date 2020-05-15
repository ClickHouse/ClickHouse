---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "Aritm\xE9tica"
---

# Funciones aritméticas {#arithmetic-functions}

Para todas las funciones aritméticas, el tipo de resultado se calcula como el tipo de número más pequeño en el que encaja el resultado, si existe dicho tipo. El mínimo se toma simultáneamente en función del número de bits, si está firmado y si flota. Si no hay suficientes bits, se toma el tipo de bit más alto.

Ejemplo:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Las funciones aritméticas funcionan para cualquier par de tipos de UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32 o Float64.

El desbordamiento se produce de la misma manera que en C ++.

## más (a, b), a + b operador {#plusa-b-a-b-operator}

Calcula la suma de los números.
También puede agregar números enteros con una fecha o fecha y hora. En el caso de una fecha, agregar un entero significa agregar el número correspondiente de días. Para una fecha con hora, significa agregar el número correspondiente de segundos.

## menos(a, b), a - b operador {#minusa-b-a-b-operator}

Calcula la diferencia. El resultado siempre está firmado.

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## multiplicar(a, b) a \* b operador {#multiplya-b-a-b-operator}

Calcula el producto de los números.

## divide (a, b), operador a / b {#dividea-b-a-b-operator}

Calcula el cociente de los números. El tipo de resultado es siempre un tipo de punto flotante.
No es una división entera. Para la división de enteros, use el ‘intDiv’ función.
Al dividir por cero obtienes ‘inf’, ‘-inf’, o ‘nan’.

## Información de uso) {#intdiva-b}

Calcula el cociente de los números. Se divide en enteros, redondeando hacia abajo (por el valor absoluto).
Se produce una excepción al dividir por cero o al dividir un número negativo mínimo por menos uno.

## IntDivOrZero (a, b) {#intdivorzeroa-b}

Difiere de ‘intDiv’ en que devuelve cero al dividir por cero o al dividir un número negativo mínimo por menos uno.

## modulo(a, b), a % b operador {#moduloa-b-a-b-operator}

Calcula el resto después de la división.
Si los argumentos son números de coma flotante, se convierten previamente en enteros eliminando la parte decimal.
El resto se toma en el mismo sentido que en C ++. La división truncada se usa para números negativos.
Se produce una excepción al dividir por cero o al dividir un número negativo mínimo por menos uno.

## moduloOrZero (a, b) {#moduloorzeroa-b}

Difiere de ‘modulo’ en que devuelve cero cuando el divisor es cero.

## negate(a), -un operador {#negatea-a-operator}

Calcula un número con el signo inverso. El resultado siempre está firmado.

## abs (a) {#arithm_func-abs}

Calcula el valor absoluto del número (a). Es decir, si un \<0, devuelve -a . Para los tipos sin firmar no hace nada. Para los tipos de enteros con signo, devuelve un número sin signo.

## GCD (a, b) {#gcda-b}

Devuelve el mayor divisor común de los números.
Se produce una excepción al dividir por cero o al dividir un número negativo mínimo por menos uno.

## Lcm(a, b) {#lcma-b}

Devuelve el mínimo múltiplo común de los números.
Se produce una excepción al dividir por cero o al dividir un número negativo mínimo por menos uno.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
