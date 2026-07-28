---
description: 'Documentación de Clickhouse Obfuscator'
slug: /operations/utilities/clickhouse-obfuscator
title: 'clickhouse-obfuscator'
doc_type: 'reference'
---

Una herramienta sencilla para la ofuscación de datos de tablas.

Lee una tabla de entrada y genera una tabla de salida que conserva algunas propiedades de la original, pero contiene datos diferentes.
Permite publicar datos de producción casi reales para su uso en benchmark.

Está diseñada para conservar las siguientes propiedades de los datos:

* cardinalidades de valores (número de valores distintos) para cada columna y cada tupla de columnas;

* cardinalidades condicionales: número de valores distintos de una columna en función del valor de otra columna;

* distribuciones de probabilidad del valor absoluto de los enteros; del signo de los enteros con signo; y del exponente y el signo en los flotantes;

* distribuciones de probabilidad de la longitud de las cadenas;

* probabilidad de valores numéricos cero; cadenas y arrays vacíos; valores `NULL`;

* ratio de compresión de los datos al comprimirse con LZ77 y con la familia de códec entropy;

* continuidad (magnitud de la diferencia) de los valores temporales en toda la tabla; continuidad de los valores de coma flotante;

* componente de fecha de los valores `DateTime`;

* validez UTF-8 de los valores de cadena;

* los valores de cadena tienen un aspecto natural.

La mayoría de las propiedades anteriores son útiles para las pruebas de rendimiento:

la lectura de datos, el filtrado, la aggregation y la ordenación funcionarán casi a la misma velocidad
que con los datos originales gracias a que se conservan las cardinalidades, magnitudes, ratios de compresión, etc.

Funciona de forma determinista: se define un valor seed y la transformación queda determinada por los datos de entrada y por el seed.
Algunas transformaciones son biyectivas y podrían revertirse, por lo que necesita usar un seed grande y mantenerlo en secreto.

Utiliza algunos primitives criptográficos para transformar datos, pero desde el punto de vista criptográfico no lo hace correctamente; por eso, no debe considerar el resultado como seguro, salvo que tenga otro motivo para hacerlo. El resultado puede conservar algunos datos que no quiera publicar.

Siempre deja los números 0, 1 y -1, las fechas, las longitudes de los arrays y los indicadores de nulos exactamente igual que en los datos de origen.
Por ejemplo, si tiene una columna `IsMobile` en su tabla con valores 0 y 1, en los datos transformados tendrá el mismo valor.

Así, el usuario podrá calcular la ratio exacta del tráfico móvil.

Veamos otro ejemplo. Si tiene datos privados en su tabla, como el correo electrónico de un usuario, y no quiere publicar ninguna dirección de correo concreta.
Si su tabla es lo bastante grande y contiene varios correos electrónicos distintos, y ninguno de ellos tiene una frecuencia mucho mayor que la de los demás, anonimizará todos los datos. Pero si tiene un número reducido de valores distintos en una columna, puede reproducir algunos de ellos.
Debería revisar cómo funciona el algoritmo de esta herramienta y ajustar con precisión sus parámetros de línea de comandos.

Esta herramienta solo funciona bien con al menos una cantidad moderada de datos (como mínimo, miles de filas).