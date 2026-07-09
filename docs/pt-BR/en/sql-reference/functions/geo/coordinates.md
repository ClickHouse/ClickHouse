---
description: 'Documentação sobre coordenadas'
sidebar_label: 'Coordenadas geográficas'
slug: /sql-reference/functions/geo/coordinates
title: 'Funções para trabalhar com coordenadas geográficas'
doc_type: 'reference'
---

<div id="greatcircledistance">
  ## greatCircleDistance
</div>

Calcula a distância entre dois pontos na superfície terrestre usando [a fórmula do círculo máximo](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parâmetros de entrada**

* `lon1Deg` — Longitude do primeiro ponto em graus. Intervalo: `[-180°, 180°]`.
* `lat1Deg` — Latitude do primeiro ponto em graus. Intervalo: `[-90°, 90°]`.
* `lon2Deg` — Longitude do segundo ponto em graus. Intervalo: `[-180°, 180°]`.
* `lat2Deg` — Latitude do segundo ponto em graus. Intervalo: `[-90°, 90°]`.

Valores positivos correspondem à latitude Norte e à longitude Leste, e valores negativos correspondem à latitude Sul e à longitude Oeste.

**Valor retornado**

A distância entre dois pontos na superfície da Terra, em metros.

Gera uma exceção quando os valores dos parâmetros de entrada estão fora do intervalo.

**Exemplo**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673) AS greatCircleDistance
```

```text
┌─greatCircleDistance─┐
│            14128352 │
└─────────────────────┘
```

<div id="geodistance">
  ## geoDistance
</div>

Semelhante a `greatCircleDistance`, mas calcula a distância no elipsoide WGS-84 em vez de na esfera. Essa é uma aproximação mais precisa do geoide da Terra.
O desempenho é o mesmo que o de `greatCircleDistance` (sem perda de desempenho). Recomenda-se usar `geoDistance` para calcular distâncias na Terra.

Nota técnica: para pontos suficientemente próximos, calculamos a distância usando uma aproximação planar com a métrica no plano tangente ao ponto médio das coordenadas.

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parâmetros de entrada**

* `lon1Deg` — Longitude do primeiro ponto em graus. Intervalo: `[-180°, 180°]`.
* `lat1Deg` — Latitude do primeiro ponto em graus. Intervalo: `[-90°, 90°]`.
* `lon2Deg` — Longitude do segundo ponto em graus. Intervalo: `[-180°, 180°]`.
* `lat2Deg` — Latitude do segundo ponto em graus. Intervalo: `[-90°, 90°]`.

Valores positivos correspondem à latitude norte e à longitude leste, e valores negativos correspondem à latitude sul e à longitude oeste.

**Valor retornado**

A distância entre dois pontos na superfície da Terra, em metros.

Gera uma exceção quando os valores dos parâmetros de entrada estão fora do intervalo.

**Exemplo**

```sql
SELECT geoDistance(38.8976, -77.0366, 39.9496, -75.1503) AS geoDistance
```

```text
┌─geoDistance─┐
│   212458.73 │
└─────────────┘
```

<div id="greatcircleangle">
  ## greatCircleAngle
</div>

Calcula o ângulo central entre dois pontos na superfície da Terra usando [a fórmula do círculo máximo](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parâmetros de entrada**

* `lon1Deg` — Longitude do primeiro ponto em graus.
* `lat1Deg` — Latitude do primeiro ponto em graus.
* `lon2Deg` — Longitude do segundo ponto em graus.
* `lat2Deg` — Latitude do segundo ponto em graus.

**Valor de retorno**

O ângulo central entre dois pontos, em graus.

**Exemplo**

```sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

```text
┌─arc─┐
│  45 │
└─────┘
```

<div id="geotoutm">
  ## geoToUTM
</div>

Converte coordenadas geográficas WGS84 `(longitude, latitude)` em coordenadas [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system).

O UTM é um conjunto de 60 projeções transversas de Mercator, cada uma cobrindo uma zona longitudinal de 6° de largura, que mapeiam coordenadas geográficas para uma grade plana em metros. A zona é selecionada automaticamente com base na longitude, aplicando as exceções padrão para a Noruega e Svalbard, a menos que uma `zone` explícita seja especificada. O UTM é definido apenas para latitudes no intervalo `[-80°, 84°]`; as calotas polares usam o sistema UPS separado.

```sql
geoToUTM(longitude, latitude[, zone])
```

**Argumentos**

* `longitude` — Longitude em graus. Intervalo: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — Latitude em graus. Intervalo: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `zone` — Opcional. Força a projeção nesta zona UTM em vez de selecioná-la automaticamente. Intervalo: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).

**Valor retornado**

Uma tupla nomeada `(easting, northing, zone, band)`: `easting` e `northing` em metros ([`Float64`](../../data-types/float.md)), o número da zona UTM `zone` ([`UInt8`](../../data-types/int-uint.md)) e a letra da banda de latitude MGRS `band` ([`FixedString(1)`](../../data-types/fixedstring.md)). Um `band` de `'N'` em diante indica o hemisfério norte.

Gera uma exceção quando a latitude está fora de `[-80°, 84°]` ou a longitude está fora de `[-180°, 180°]`.

**Exemplo**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

<div id="utmtogeo">
  ## UTMToGeo
</div>

Converte coordenadas [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) em coordenadas geográficas WGS84 `(longitude, latitude)`. Esta é a operação inversa de [`geoToUTM`](#geotoutm).

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**Argumentos**

* `easting` — Coordenada leste em metros (inclui o false easting de 500000 m). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `northing` — Coordenada norte em metros (inclui o false northing de 10000000 m no hemisfério sul). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `zone` — Número da zona UTM. Intervalo: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).
* `is_north` — Hemisfério: `1` para o hemisfério norte, `0` para o hemisfério sul. [`(U)Int*`](../../data-types/int-uint.md).

**Valor retornado**

Uma tupla nomeada `(longitude, latitude)` em graus. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Exemplo**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

<div id="geotomgrs">
  ## geoToMGRS
</div>

Codifica coordenadas geográficas WGS84 `(longitude, latitude)` como uma string no formato do [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System).

A string tem o formato `<zone><band><100km square><easting><northing>`, por exemplo `31UDQ4825111935`. O argumento `precision` controla o número de dígitos usados em cada um dos componentes easting e northing: `5` (padrão) para 1 m, `4` para 10 m, `3` para 100 m, `2` para 1 km, `1` para 10 km e `0` apenas para o quadrado da grade de 100 km. O MGRS só é definido para latitudes no intervalo `[-80°, 84°]`.

```sql
geoToMGRS(longitude, latitude[, precision])
```

**Argumentos**

* `longitude` — Longitude em graus. Intervalo: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — Latitude em graus. Intervalo: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `precision` — Opcional. Número de dígitos para cada uma das coordenadas leste e norte. Padrão: `5`. Intervalo: `[0, 5]`. [`(U)Int*`](../../data-types/int-uint.md).

**Valor retornado**

A referência MGRS em formato de string. [`String`](../../data-types/string.md).

**Exemplo**

```sql
SELECT geoToMGRS(2.294497, 48.858222) AS mgrs, geoToMGRS(2.294497, 48.858222, 3) AS mgrs_100m;
```

```text
┌─mgrs────────────┬─mgrs_100m───┐
│ 31UDQ4825111935 │ 31UDQ482119 │
└─────────────────┴─────────────┘
```

<div id="mgrstogeo">
  ## MGRSToGeo
</div>

Decodifica uma string [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) em coordenadas geográficas WGS84 `(longitude, latitude)`. Este é o inverso de [`geoToMGRS`](#geotomgrs).

O ponto retornado é o centro do quadrado da grade referenciado, portanto a precisão do resultado corresponde à precisão codificada na string. Os espaços em branco na entrada são ignorados, e as letras não diferenciam maiúsculas de minúsculas.

```sql
MGRSToGeo(mgrs)
```

**Argumentos**

* `mgrs` — Cadeia de referência MGRS a ser decodificada. [`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md).

**Valor retornado**

Uma tupla nomeada `(longitude, latitude)` em graus. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Exemplo**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

<div id="pointinellipses">
  ## pointInEllipses
</div>

Verifica se o ponto pertence a pelo menos uma das elipses.
As coordenadas são geométricas e seguem o sistema de coordenadas cartesianas.

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Parâmetros de entrada**

* `x, y` — Coordenadas de um ponto no plano.
* `xᵢ, yᵢ` — Coordenadas do centro da `i`-ésima elipse.
* `aᵢ, bᵢ` — Eixos da `i`-ésima elipse nas unidades das coordenadas `x` e `y`.

A quantidade de parâmetros de entrada deve ser `2+4⋅n`, em que `n` é o número de elipses.

**Valores retornados**

`1` se o ponto estiver dentro de pelo menos uma das elipses; `0` se não estiver.

**Exemplo**

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

<div id="pointinpolygon">
  ## pointInPolygon
</div>

Verifica se o ponto pertence ao polígono no plano.

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Valores de entrada**

* `(x, y)` — Coordenadas de um ponto no plano. Tipo de dado — [Tuple](../../data-types/tuple.md) — Uma tupla de dois números.
* `[(a, b), (c, d) ...]` — Vértices do polígono. Tipo de dado — [Array](../../data-types/array.md). Cada vértice é representado por um par de coordenadas `(a, b)`. Os vértices devem ser especificados em sentido horário ou anti-horário. O número mínimo de vértices é 3. O polígono deve ser constante.
* A função oferece suporte a polígonos com buracos (seções recortadas). Tipo de dado — [Polygon](../../data-types/geo.md/#polygon). Passe o `Polygon` inteiro como segundo argumento ou passe primeiro o anel externo e, em seguida, cada buraco como argumento adicional separado.
* A função também oferece suporte a multipolígono. Tipo de dado — [MultiPolygon](../../data-types/geo.md/#multipolygon). Passe o `MultiPolygon` inteiro como segundo argumento ou liste cada polígono componente como um argumento separado.

**Valores retornados**

`1` se o ponto estiver dentro do polígono, `0` caso contrário.
Se o ponto estiver na borda do polígono, a função poderá retornar 0 ou 1.

**Exemplo**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

> **Nota**
> • Você pode definir `validate_polygons = 0` para ignorar a validação de geometria.
> • `pointInPolygon` pressupõe que todo polígono esteja bem formado. Se a entrada tiver auto-interseções, anéis em ordem incorreta ou arestas sobrepostas, os resultados se tornam pouco confiáveis — especialmente para pontos que ficam exatamente sobre uma aresta, um vértice ou dentro de uma auto-interseção, em que a noção de &quot;dentro&quot; versus &quot;fora&quot; é indefinida.
> • Quando o argumento de polígono é constante e o ponto é expresso usando colunas-chave indexadas (por exemplo, `pointInPolygon((x, y), constant_polygon)` em uma tabela em que `x, y` fazem parte da `PRIMARY KEY` ou são cobertos por um índice `minmax`), o ClickHouse pode usar tanto a chave primária quanto os índices `minmax` de data-skipping para descartar grânulos irrelevantes.