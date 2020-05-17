---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 68
toc_title: "C\xF3mo escribir c\xF3digo C ++"
---

# Cómo escribir código C ++ {#how-to-write-c-code}

## Recomendaciones generales {#general-recommendations}

**1.** Las siguientes son recomendaciones, no requisitos.

**2.** Si está editando código, tiene sentido seguir el formato del código existente.

**3.** El estilo de código es necesario para la coherencia. La consistencia facilita la lectura del código y también facilita la búsqueda del código.

**4.** Muchas de las reglas no tienen razones lógicas; están dictadas por prácticas establecidas.

## Formatear {#formatting}

**1.** La mayor parte del formato se realizará automáticamente por `clang-format`.

**2.** Las sangrías son 4 espacios. Configure el entorno de desarrollo para que una pestaña agregue cuatro espacios.

**3.** Abrir y cerrar llaves deben estar en una línea separada.

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** Si todo el cuerpo de la función es `statement`, se puede colocar en una sola línea. Coloque espacios alrededor de llaves (además del espacio al final de la línea).

``` cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** Para funciones. No coloque espacios alrededor de los corchetes.

``` cpp
void reinsert(const Value & x)
```

``` cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** En `if`, `for`, `while` y otras expresiones, se inserta un espacio delante del corchete de apertura (a diferencia de las llamadas a funciones).

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** Agregar espacios alrededor de los operadores binarios (`+`, `-`, `*`, `/`, `%`, …) and the ternary operator `?:`.

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** Si se introduce un avance de línea, coloque al operador en una nueva línea y aumente la sangría antes de ella.

``` cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** Puede utilizar espacios para la alineación dentro de una línea, si lo desea.

``` cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** No use espacios alrededor de los operadores `.`, `->`.

Si es necesario, el operador se puede envolver a la siguiente línea. En este caso, el desplazamiento frente a él aumenta.

**11.** No utilice un espacio para separar los operadores unarios (`--`, `++`, `*`, `&`, …) from the argument.

**12.** Pon un espacio después de una coma, pero no antes. La misma regla se aplica a un punto y coma dentro de un `for` expresion.

**13.** No utilice espacios para separar el `[]` operador.

**14.** En un `template <...>` expresión, use un espacio entre `template` y `<`; sin espacios después de `<` o antes `>`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** En clases y estructuras, escribe `public`, `private`, y `protected` en el mismo nivel que `class/struct`, y sangrar el resto del código.

``` cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** Si el mismo `namespace` se usa para todo el archivo, y no hay nada más significativo, no es necesario un desplazamiento dentro `namespace`.

**17.** Si el bloque para un `if`, `for`, `while`, u otra expresión consiste en una sola `statement`, las llaves son opcionales. Coloque el `statement` en una línea separada, en su lugar. Esta regla también es válida para `if`, `for`, `while`, …

Pero si el interior `statement` contiene llaves o `else`, el bloque externo debe escribirse entre llaves.

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** No debería haber espacios al final de las líneas.

**19.** Los archivos de origen están codificados en UTF-8.

**20.** Los caracteres no ASCII se pueden usar en literales de cadena.

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** No escriba varias expresiones en una sola línea.

**22.** Agrupe secciones de código dentro de las funciones y sepárelas con no más de una línea vacía.

**23.** Separe funciones, clases, etc. con una o dos líneas vacías.

**24.** `A const` (relacionado con un valor) debe escribirse antes del nombre del tipo.

``` cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** Al declarar un puntero o referencia, el `*` y `&` Los símbolos deben estar separados por espacios en ambos lados.

``` cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Cuando utilice tipos de plantilla, alias con el `using` palabra clave (excepto en los casos más simples).

En otras palabras, los parámetros de la plantilla se especifican solo en `using` y no se repiten en el código.

`using` se puede declarar localmente, como dentro de una función.

``` cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** No declare varias variables de diferentes tipos en una instrucción.

``` cpp
//incorrect
int x, *y;
```

**28.** No utilice moldes de estilo C.

``` cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** En clases y estructuras, los miembros del grupo y las funciones por separado dentro de cada ámbito de visibilidad.

**30.** Para clases y estructuras pequeñas, no es necesario separar la declaración del método de la implementación.

Lo mismo es cierto para los métodos pequeños en cualquier clase o estructura.

Para clases y estructuras con plantillas, no separe las declaraciones de métodos de la implementación (porque de lo contrario deben definirse en la misma unidad de traducción).

**31.** Puede ajustar líneas en 140 caracteres, en lugar de 80.

**32.** Utilice siempre los operadores de incremento / decremento de prefijo si no se requiere postfix.

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## Comentario {#comments}

**1.** Asegúrese de agregar comentarios para todas las partes no triviales del código.

Esto es muy importante. Escribir el comentario puede ayudarte a darte cuenta de que el código no es necesario o que está diseñado incorrectamente.

``` cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Los comentarios pueden ser tan detallados como sea necesario.

**3.** Coloque comentarios antes del código que describen. En casos raros, los comentarios pueden aparecer después del código, en la misma línea.

``` cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// Here could be written the description on how query was executed
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// Up to which stage process the SELECT query
    )
```

**4.** Los comentarios deben escribirse en inglés solamente.

**5.** Si está escribiendo una biblioteca, incluya comentarios detallados que la expliquen en el archivo de encabezado principal.

**6.** No agregue comentarios que no proporcionen información adicional. En particular, no deje comentarios vacíos como este:

``` cpp
/*
* Procedure Name:
* Original procedure name:
* Author:
* Date of creation:
* Dates of modification:
* Modification authors:
* Original file name:
* Purpose:
* Intent:
* Designation:
* Classes used:
* Constants:
* Local variables:
* Parameters:
* Date of creation:
* Purpose:
*/
```

El ejemplo se toma prestado del recurso http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/.

**7.** No escriba comentarios de basura (autor, fecha de creación ..) al principio de cada archivo.

**8.** Los comentarios de una sola línea comienzan con tres barras: `///` y los comentarios de varias líneas comienzan con `/**`. Estos comentarios son considerados “documentation”.

Nota: Puede usar Doxygen para generar documentación a partir de estos comentarios. Pero Doxygen no se usa generalmente porque es más conveniente navegar por el código en el IDE.

**9.** Los comentarios de varias líneas no deben tener líneas vacías al principio y al final (excepto la línea que cierra un comentario de varias líneas).

**10.** Para comentar el código, use comentarios básicos, no “documenting” comentario.

**11.** Elimine las partes comentadas del código antes de confirmar.

**12.** No use blasfemias en comentarios o código.

**13.** No use letras mayúsculas. No use puntuación excesiva.

``` cpp
/// WHAT THE FAIL???
```

**14.** No use comentarios para hacer delímetros.

``` cpp
///******************************************************
```

**15.** No comiencen las discusiones en los comentarios.

``` cpp
/// Why did you do this stuff?
```

**16.** No es necesario escribir un comentario al final de un bloque que describa de qué se trataba.

``` cpp
/// for
```

## Nombre {#names}

**1.** Use letras minúsculas con guiones bajos en los nombres de variables y miembros de clase.

``` cpp
size_t max_block_size;
```

**2.** Para los nombres de las funciones (métodos), use camelCase comenzando con una letra minúscula.

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** Para los nombres de las clases (estructuras), use CamelCase comenzando con una letra mayúscula. Los prefijos distintos de I no se usan para interfaces.

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` se nombran de la misma manera que las clases, o con `_t` al final.

**5.** Nombres de argumentos de tipo de plantilla: en casos simples, use `T`; `T`, `U`; `T1`, `T2`.

Para casos más complejos, siga las reglas para los nombres de clase o agregue el prefijo `T`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Nombres de argumentos constantes de plantilla: siga las reglas para los nombres de variables o use `N` en casos simples.

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** Para clases abstractas (interfaces) puede agregar el `I` prefijo.

``` cpp
class IBlockInputStream
```

**8.** Si usa una variable localmente, puede usar el nombre corto.

En todos los demás casos, use un nombre que describa el significado.

``` cpp
bool info_successfully_loaded = false;
```

**9.** Nombres de `define`s y las constantes globales usan ALL\_CAPS con guiones bajos.

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** Los nombres de archivo deben usar el mismo estilo que su contenido.

Si un archivo contiene una sola clase, nombre el archivo de la misma manera que la clase (CamelCase).

Si el archivo contiene una sola función, nombre el archivo de la misma manera que la función (camelCase).

**11.** Si el nombre contiene una abreviatura, :

-   Para los nombres de variables, la abreviatura debe usar letras minúsculas `mysql_connection` (ni `mySQL_connection`).
-   Para los nombres de clases y funciones, mantenga las letras mayúsculas en la abreviatura`MySQLConnection` (ni `MySqlConnection`).

**12.** Los argumentos del constructor que se usan solo para inicializar los miembros de la clase deben nombrarse de la misma manera que los miembros de la clase, pero con un guión bajo al final.

``` cpp
FileQueueProcessor(
    const std::string & path_,
    const std::string & prefix_,
    std::shared_ptr<FileHandler> handler_)
    : path(path_),
    prefix(prefix_),
    handler(handler_),
    log(&Logger::get("FileQueueProcessor"))
{
}
```

El sufijo de subrayado se puede omitir si el argumento no se usa en el cuerpo del constructor.

**13.** No hay diferencia en los nombres de las variables locales y los miembros de la clase (no se requieren prefijos).

``` cpp
timer (not m_timer)
```

**14.** Para las constantes en un `enum`, usar CamelCase con una letra mayúscula. ALL\_CAPS también es aceptable. Si el `enum` no es local, utilice un `enum class`.

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** Todos los nombres deben estar en inglés. La transliteración de palabras rusas no está permitida.

    not Stroka

**16.** Las abreviaturas son aceptables si son bien conocidas (cuando puede encontrar fácilmente el significado de la abreviatura en Wikipedia o en un motor de búsqueda).

    `AST`, `SQL`.

    Not `NVDH` (some random letters)

Las palabras incompletas son aceptables si la versión abreviada es de uso común.

También puede usar una abreviatura si el nombre completo se incluye junto a él en los comentarios.

**17.** Los nombres de archivo con código fuente de C++ deben tener `.cpp` ampliación. Los archivos de encabezado deben tener `.h` ampliación.

## Cómo escribir código {#how-to-write-code}

**1.** Gestión de la memoria.

Desasignación de memoria manual (`delete`) solo se puede usar en el código de la biblioteca.

En el código de la biblioteca, el `delete` operador sólo se puede utilizar en destructores.

En el código de la aplicación, la memoria debe ser liberada por el objeto que la posee.

Ejemplos:

-   La forma más fácil es colocar un objeto en la pila o convertirlo en miembro de otra clase.
-   Para una gran cantidad de objetos pequeños, use contenedores.
-   Para la desasignación automática de un pequeño número de objetos que residen en el montón, use `shared_ptr/unique_ptr`.

**2.** Gestión de recursos.

Utilizar `RAII` y ver arriba.

**3.** Manejo de errores.

Utilice excepciones. En la mayoría de los casos, solo necesita lanzar una excepción y no necesita atraparla (debido a `RAII`).

En las aplicaciones de procesamiento de datos fuera de línea, a menudo es aceptable no detectar excepciones.

En los servidores que manejan las solicitudes de los usuarios, generalmente es suficiente detectar excepciones en el nivel superior del controlador de conexión.

En las funciones de subproceso, debe capturar y mantener todas las excepciones para volver a lanzarlas en el subproceso principal después `join`.

``` cpp
/// If there weren't any calculations yet, calculate the first block synchronously
if (!started)
{
    calculate();
    started = true;
}
else /// If calculations are already in progress, wait for the result
    pool.wait();

if (exception)
    exception->rethrow();
```

Nunca oculte excepciones sin manejo. Nunca simplemente ponga ciegamente todas las excepciones para iniciar sesión.

``` cpp
//Not correct
catch (...) {}
```

Si necesita ignorar algunas excepciones, hágalo solo para las específicas y vuelva a lanzar el resto.

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

Al usar funciones con códigos de respuesta o `errno`, siempre verifique el resultado y arroje una excepción en caso de error.

``` cpp
if (0 != close(fd))
    throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
```

`Do not use assert`.

**4.** Tipos de excepción.

No es necesario utilizar una jerarquía de excepciones compleja en el código de la aplicación. El texto de excepción debe ser comprensible para un administrador del sistema.

**5.** Lanzar excepciones de destructores.

Esto no es recomendable, pero está permitido.

Utilice las siguientes opciones:

-   Crear una función (`done()` o `finalize()`) que hará todo el trabajo de antemano que podría conducir a una excepción. Si se llamó a esa función, no debería haber excepciones en el destructor más adelante.
-   Las tareas que son demasiado complejas (como enviar mensajes a través de la red) se pueden poner en un método separado al que el usuario de la clase tendrá que llamar antes de la destrucción.
-   Si hay una excepción en el destructor, es mejor registrarla que ocultarla (si el registrador está disponible).
-   En aplicaciones simples, es aceptable confiar en `std::terminate` (para los casos de `noexcept` de forma predeterminada en C ++ 11) para manejar excepciones.

**6.** Bloques de código anónimos.

Puede crear un bloque de código separado dentro de una sola función para hacer que ciertas variables sean locales, de modo que se llame a los destructores al salir del bloque.

``` cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multithreading.

En programas de procesamiento de datos fuera de línea:

-   Trate de obtener el mejor rendimiento posible en un solo núcleo de CPU. A continuación, puede paralelizar su código si es necesario.

En aplicaciones de servidor:

-   Utilice el grupo de subprocesos para procesar solicitudes. En este punto, no hemos tenido ninguna tarea que requiera el cambio de contexto de espacio de usuario.

La horquilla no se usa para la paralelización.

**8.** Sincronización de hilos.

A menudo es posible hacer que diferentes hilos usen diferentes celdas de memoria (incluso mejor: diferentes líneas de caché) y no usar ninguna sincronización de hilos (excepto `joinAll`).

Si se requiere sincronización, en la mayoría de los casos, es suficiente usar mutex bajo `lock_guard`.

En otros casos, use primitivas de sincronización del sistema. No utilice la espera ocupada.

Las operaciones atómicas deben usarse solo en los casos más simples.

No intente implementar estructuras de datos sin bloqueo a menos que sea su principal área de especialización.

**9.** Punteros vs referencias.

En la mayoría de los casos, prefiera referencias.

**10.** Construir.

Usar referencias constantes, punteros a constantes, `const_iterator`, y métodos const.

Considerar `const` para ser predeterminado y usar no-`const` sólo cuando sea necesario.

Al pasar variables por valor, usando `const` por lo general no tiene sentido.

**11.** sin firmar.

Utilizar `unsigned` si es necesario.

**12.** Tipos numéricos.

Utilice los tipos `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, y `Int64`, así como `size_t`, `ssize_t`, y `ptrdiff_t`.

No use estos tipos para números: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** Pasando argumentos.

Pasar valores complejos por referencia (incluyendo `std::string`).

Si una función captura la propiedad de un objeto creado en el montón, cree el tipo de argumento `shared_ptr` o `unique_ptr`.

**14.** Valores devueltos.

En la mayoría de los casos, sólo tiene que utilizar `return`. No escribir `[return std::move(res)]{.strike}`.

Si la función asigna un objeto en el montón y lo devuelve, use `shared_ptr` o `unique_ptr`.

En casos excepcionales, es posible que deba devolver el valor a través de un argumento. En este caso, el argumento debe ser una referencia.

``` cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** espacio de nombres.

No hay necesidad de usar un `namespace` para el código de aplicación.

Las bibliotecas pequeñas tampoco necesitan esto.

Para bibliotecas medianas a grandes, coloque todo en un `namespace`.

En la biblioteca `.h` archivo, se puede utilizar `namespace detail` para ocultar los detalles de implementación no necesarios para el código de la aplicación.

En un `.cpp` archivo, puede usar un `static` o espacio de nombres anónimo para ocultar símbolos.

Además, un `namespace` puede ser utilizado para un `enum` para evitar que los nombres correspondientes caigan en un `namespace` (pero es mejor usar un `enum class`).

**16.** Inicialización diferida.

Si se requieren argumentos para la inicialización, normalmente no debe escribir un constructor predeterminado.

Si más adelante tendrá que retrasar la inicialización, puede agregar un constructor predeterminado que creará un objeto no válido. O, para un pequeño número de objetos, puede usar `shared_ptr/unique_ptr`.

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** Funciones virtuales.

Si la clase no está destinada para uso polimórfico, no necesita hacer que las funciones sean virtuales. Esto también se aplica al destructor.

**18.** Codificación.

Usa UTF-8 en todas partes. Utilizar `std::string`y`char *`. No use `std::wstring`y`wchar_t`.

**19.** Tala.

Vea los ejemplos en todas partes del código.

Antes de confirmar, elimine todo el registro de depuración y sin sentido, y cualquier otro tipo de salida de depuración.

Se debe evitar el registro en ciclos, incluso en el nivel Trace.

Los registros deben ser legibles en cualquier nivel de registro.

El registro solo debe usarse en el código de la aplicación, en su mayor parte.

Los mensajes de registro deben estar escritos en inglés.

El registro debe ser preferiblemente comprensible para el administrador del sistema.

No use blasfemias en el registro.

Utilice la codificación UTF-8 en el registro. En casos excepcionales, puede usar caracteres que no sean ASCII en el registro.

**20.** Entrada-salida.

No utilice `iostreams` en ciclos internos que son críticos para el rendimiento de la aplicación (y nunca usan `stringstream`).

Utilice el `DB/IO` biblioteca en su lugar.

**21.** Fecha y hora.

Ver el `DateLUT` biblioteca.

**22.** incluir.

Utilice siempre `#pragma once` en lugar de incluir guardias.

**23.** utilizar.

`using namespace` no se utiliza. Usted puede utilizar `using` con algo específico. Pero hazlo local dentro de una clase o función.

**24.** No use `trailing return type` para funciones a menos que sea necesario.

``` cpp
[auto f() -&gt; void;]{.strike}
```

**25.** Declaración e inicialización de variables.

``` cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** Para funciones virtuales, escriba `virtual` en la clase base, pero escribe `override` en lugar de `virtual` en las clases descendientes.

## Características no utilizadas de C ++ {#unused-features-of-c}

**1.** La herencia virtual no se utiliza.

**2.** Los especificadores de excepción de C ++ 03 no se usan.

## Plataforma {#platform}

**1.** Escribimos código para una plataforma específica.

Pero en igualdad de condiciones, se prefiere el código multiplataforma o portátil.

**2.** Idioma: C ++ 17.

**3.** Compilación: `gcc`. En este momento (diciembre de 2017), el código se compila utilizando la versión 7.2. (También se puede compilar usando `clang 4`.)

Se utiliza la biblioteca estándar (`libstdc++` o `libc++`).

**4.**OS: Linux Ubuntu, no más viejo que Precise.

**5.**El código está escrito para la arquitectura de CPU x86\_64.

El conjunto de instrucciones de CPU es el conjunto mínimo admitido entre nuestros servidores. Actualmente, es SSE 4.2.

**6.** Utilizar `-Wall -Wextra -Werror` flags de compilación.

**7.** Use enlaces estáticos con todas las bibliotecas, excepto aquellas a las que son difíciles de conectar estáticamente (consulte la salida de la `ldd` comando).

**8.** El código se desarrolla y se depura con la configuración de la versión.

## Herramienta {#tools}

**1.** KDevelop es un buen IDE.

**2.** Para la depuración, use `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...`, o `tcmalloc_minimal_debug`.

**3.** Para crear perfiles, use `Linux Perf`, `valgrind` (`callgrind`), o `strace -cf`.

**4.** Las fuentes están en Git.

**5.** Usos de ensamblaje `CMake`.

**6.** Los programas se lanzan usando `deb` paquete.

**7.** Los compromisos a dominar no deben romper la compilación.

Aunque solo las revisiones seleccionadas se consideran viables.

**8.** Realice confirmaciones tan a menudo como sea posible, incluso si el código está parcialmente listo.

Use ramas para este propósito.

Si su código en el `master` branch todavía no se puede construir, excluirlo de la compilación antes de la `push`. Tendrá que terminarlo o eliminarlo dentro de unos días.

**9.** Para cambios no triviales, use ramas y publíquelas en el servidor.

**10.** El código no utilizado se elimina del repositorio.

## Biblioteca {#libraries}

**1.** Se utiliza la biblioteca estándar de C ++ 14 (se permiten extensiones experimentales), así como `boost` y `Poco` marco.

**2.** Si es necesario, puede usar cualquier biblioteca conocida disponible en el paquete del sistema operativo.

Si ya hay una buena solución disponible, úsela, incluso si eso significa que debe instalar otra biblioteca.

(Pero prepárese para eliminar las bibliotecas incorrectas del código.)

**3.** Puede instalar una biblioteca que no esté en los paquetes, si los paquetes no tienen lo que necesita o tienen una versión obsoleta o el tipo de compilación incorrecto.

**4.** Si la biblioteca es pequeña y no tiene su propio sistema de compilación complejo, coloque los archivos `contrib` carpeta.

**5.** Siempre se da preferencia a las bibliotecas que ya están en uso.

## Recomendaciones generales {#general-recommendations-1}

**1.** Escribe el menor código posible.

**2.** Pruebe la solución más simple.

**3.** No escriba código hasta que sepa cómo va a funcionar y cómo funcionará el bucle interno.

**4.** En los casos más simples, use `using` en lugar de clases o estructuras.

**5.** Si es posible, no escriba constructores de copia, operadores de asignación, destructores (que no sean virtuales, si la clase contiene al menos una función virtual), mueva constructores o mueva operadores de asignación. En otras palabras, las funciones generadas por el compilador deben funcionar correctamente. Usted puede utilizar `default`.

**6.** Se fomenta la simplificación del código. Reduzca el tamaño de su código siempre que sea posible.

## Recomendaciones adicionales {#additional-recommendations}

**1.** Especificar explícitamente `std::` para tipos de `stddef.h`

no se recomienda. En otras palabras, recomendamos escribir `size_t` en su lugar `std::size_t` porque es más corto.

Es aceptable agregar `std::`.

**2.** Especificar explícitamente `std::` para funciones de la biblioteca C estándar

no se recomienda. En otras palabras, escribir `memcpy` en lugar de `std::memcpy`.

La razón es que hay funciones no estándar similares, tales como `memmem`. Utilizamos estas funciones en ocasiones. Estas funciones no existen en `namespace std`.

Si usted escribe `std::memcpy` en lugar de `memcpy` en todas partes, entonces `memmem` sin `std::` se verá extraño.

Sin embargo, todavía puedes usar `std::` si lo prefieres.

**3.** Usar funciones de C cuando las mismas están disponibles en la biblioteca estándar de C ++.

Esto es aceptable si es más eficiente.

Por ejemplo, use `memcpy` en lugar de `std::copy` para copiar grandes trozos de memoria.

**4.** Argumentos de función multilínea.

Se permite cualquiera de los siguientes estilos de ajuste:

``` cpp
function(
  T1 x1,
  T2 x2)
```

``` cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

``` cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

[Artículo Original](https://clickhouse.tech/docs/en/development/style/) <!--hide-->
