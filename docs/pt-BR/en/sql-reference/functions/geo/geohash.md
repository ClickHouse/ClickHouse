---
description: 'Documentação do Geohash'
sidebar_label: 'Geohash'
slug: /sql-reference/functions/geo/geohash
title: 'Funções para trabalhar com Geohash'
doc_type: 'referência'
---

<div id="geohash">
  ## Geohash
</div>

[Geohash](https://en.wikipedia.org/wiki/Geohash) é um sistema de geocodificação que subdivide a superfície da Terra em quadrículas e codifica cada célula como uma sequência curta de letras e dígitos. Trata-se de uma estrutura de dados hierárquica, portanto, quanto mais longa for a string geohash, mais precisa será a localização geográfica.

Se você precisar converter manualmente coordenadas geográficas em strings geohash, pode usar [geohash.org](http://geohash.co/)

<div id="geohashencode">
  ## geohashEncode
</div>

Codifica latitude e longitude em uma string [geohash](#geohash).

**Sintaxe**

```sql
geohashEncode(longitude, latitude, [precision])
```

**Valores de entrada**

* `longitude` — Parte da longitude da coordenada que você quer codificar. Ponto flutuante no intervalo `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude` — Parte da latitude da coordenada que você quer codificar. Ponto flutuante no intervalo `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` (opcional) — Comprimento da string codificada resultante. O valor padrão é `12`. Inteiro no intervalo `[1, 12]`. [Int8](../../data-types/int-uint.md).

:::note

* Todos os parâmetros de coordenadas devem ser do mesmo tipo: `Float32` ou `Float64`.
* Para o parâmetro `precision`, qualquer valor menor que `1` ou maior que `12` é convertido silenciosamente em `12`.
  :::

**Valores retornados**

* String alfanumérica da coordenada codificada (usa-se uma versão modificada do alfabeto de codificação base32). [String](../../data-types/string.md).

**Exemplo**

```sql title="Query"
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

```text title="Response"
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

<div id="geohashdecode">
  ## geohashDecode
</div>

Decodifica qualquer string em [geohash](#geohash) para longitude e latitude.

**Sintaxe**

```sql
geohashDecode(hash_str)
```

**Valores de entrada**

* `hash_str` — String codificada em geohash.

**Valores retornados**

* Tuple `(longitude, latitude)` com valores `Float64` de longitude e latitude. [Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**Exemplo**

```sql
SELECT geohashDecode('ezs42') AS res;
```

```text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

<div id="geohashesinbox">
  ## geohashesInBox
</div>

Retorna um array de strings codificadas em [geohash](#geohash), com a precisão especificada, que ficam dentro da caixa fornecida e intersectam seus limites; basicamente, uma grade 2D linearizada em um array.

**Sintaxe**

```sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**Argumentos**

* `longitude_min` — Longitude mínima. Intervalo: `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_min` — Latitude mínima. Intervalo: `[-90°, 90°]`. [Float](../../data-types/float.md).
* `longitude_max` — Longitude máxima. Intervalo: `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_max` — Latitude máxima. Intervalo: `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` — Precisão do geohash. Intervalo: `[1, 12]`. [UInt8](../../data-types/int-uint.md).

:::note
Todos os parâmetros de coordenadas devem ser do mesmo tipo: `Float32` ou `Float64`.
:::

**Valores retornados**

* Array de strings de geohash-boxes, com comprimento igual à precisão, que cobrem a área fornecida; não se deve depender da ordem dos itens. [Array](../../data-types/array.md)([String](../../data-types/string.md)).
* `[]` - Array vazio se os valores mínimos de latitude e longitude não forem menores que os valores máximos correspondentes.

:::note
A função lança uma exceção se o array resultante tiver mais de 10&#39;000&#39;000 itens.
:::

**Exemplo**

```sql title="Query"
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

```text title="Response"
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```