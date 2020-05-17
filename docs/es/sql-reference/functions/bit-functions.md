---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: Trozo
---

# Bit Funciones {#bit-functions}

Las funciones de bits funcionan para cualquier par de tipos de UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32 o Float64.

El tipo de resultado es un entero con bits iguales a los bits máximos de sus argumentos. Si al menos uno de los argumentos está firmado, el resultado es un número firmado. Si un argumento es un número de coma flotante, se convierte en Int64.

## pocoY(a, b) {#bitanda-b}

## bitOr (a, b) {#bitora-b}

## ¿Por qué?) {#bitxora-b}

## ¿Por qué?) {#bitnota}

## ¿Cómo puedo hacerlo?) {#bitshiftlefta-b}

## ¿Cómo puedo hacerlo?) {#bitshiftrighta-b}

## ¿Cómo puedo hacerlo?) {#bitrotatelefta-b}

## ¿Cómo puedo hacerlo?) {#bitrotaterighta-b}

## bitTest {#bittest}

Toma cualquier entero y lo convierte en [forma binaria](https://en.wikipedia.org/wiki/Binary_number) devuelve el valor de un bit en la posición especificada. La cuenta atrás comienza desde 0 de derecha a izquierda.

**Sintaxis**

``` sql
SELECT bitTest(number, index)
```

**Parámetros**

-   `number` – integer number.
-   `index` – position of bit.

**Valores devueltos**

Devuelve un valor de bit en la posición especificada.

Tipo: `UInt8`.

**Ejemplo**

Por ejemplo, el número 43 en el sistema numérico base-2 (binario) es 101011.

Consulta:

``` sql
SELECT bitTest(43, 1)
```

Resultado:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

Otro ejemplo:

Consulta:

``` sql
SELECT bitTest(43, 2)
```

Resultado:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

Devuelve el resultado de [conjunción lógica](https://en.wikipedia.org/wiki/Logical_conjunction) (Operador AND) de todos los bits en posiciones dadas. La cuenta atrás comienza desde 0 de derecha a izquierda.

La conjucción para operaciones bit a bit:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**Sintaxis**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**Parámetros**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`) es verdadero si y solo si todas sus posiciones son verdaderas (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**Valores devueltos**

Devuelve el resultado de la conjunción lógica.

Tipo: `UInt8`.

**Ejemplo**

Por ejemplo, el número 43 en el sistema numérico base-2 (binario) es 101011.

Consulta:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5)
```

Resultado:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

Otro ejemplo:

Consulta:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2)
```

Resultado:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

Devuelve el resultado de [disyunción lógica](https://en.wikipedia.org/wiki/Logical_disjunction) (O operador) de todos los bits en posiciones dadas. La cuenta atrás comienza desde 0 de derecha a izquierda.

La disyunción para las operaciones bit a bit:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**Sintaxis**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**Parámetros**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit.

**Valores devueltos**

Devuelve el resultado de disjuction lógico.

Tipo: `UInt8`.

**Ejemplo**

Por ejemplo, el número 43 en el sistema numérico base-2 (binario) es 101011.

Consulta:

``` sql
SELECT bitTestAny(43, 0, 2)
```

Resultado:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

Otro ejemplo:

Consulta:

``` sql
SELECT bitTestAny(43, 4, 2)
```

Resultado:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## bitCount {#bitcount}

Calcula el número de bits establecido en uno en la representación binaria de un número.

**Sintaxis**

``` sql
bitCount(x)
```

**Parámetros**

-   `x` — [Entero](../../sql-reference/data-types/int-uint.md) o [punto flotante](../../sql-reference/data-types/float.md) numero. La función utiliza la representación de valor en la memoria. Permite admitir números de punto flotante.

**Valor devuelto**

-   Número de bits establecido en uno en el número de entrada.

La función no convierte el valor de entrada a un tipo más grande ([extensión de signo](https://en.wikipedia.org/wiki/Sign_extension)). Entonces, por ejemplo, `bitCount(toUInt8(-1)) = 8`.

Tipo: `UInt8`.

**Ejemplo**

Tomemos por ejemplo el número 333. Su representación binaria: 0000000101001101.

Consulta:

``` sql
SELECT bitCount(333)
```

Resultado:

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/bit_functions/) <!--hide-->
