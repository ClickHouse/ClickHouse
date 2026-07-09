---
description: 'Diretrizes de estilo de código para desenvolvimento em C++ no ClickHouse'
sidebar_label: 'Guia de Estilo de C++'
sidebar_position: 70
slug: /development/style
title: 'Guia de Estilo de C++'
doc_type: 'guide'
---

<div id="general-recommendations">
  ## Recomendações gerais
</div>

As recomendações a seguir não são requisitos.
Se você estiver editando código, faz sentido seguir a formatação do código existente.
O estilo de código é necessário para manter a consistência. A consistência facilita a leitura do código e também torna mais fácil fazer buscas no código.
Muitas das regras não têm uma razão lógica; elas são ditadas por práticas estabelecidas.

<div id="formatting">
  ## Formatação
</div>

**1.** A maior parte da formatação é feita automaticamente pelo `clang-format`.

**2.** A indentação é de 4 espaços. Configure seu ambiente de desenvolvimento para que a tecla Tab insira quatro espaços.

**3.** As chaves de abertura e fechamento devem estar em uma linha separada.

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** Se o corpo inteiro da função for um único `statement`, ele pode ser colocado em uma única linha. Coloque espaços ao redor das chaves (exceto o espaço no final da linha).

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** Para funções. Não coloque espaços ao redor dos parênteses.

```cpp
void reinsert(const Value & x)
```

```cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** Em `if`, `for`, `while` e outras expressões, um espaço é inserido antes do parêntese de abertura (ao contrário das chamadas de função).

```cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** Adicione espaços ao redor dos operadores binários (`+`, `-`, `*`, `/`, `%`, ...) e do operador ternário `?:`.

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** Se uma quebra de linha for inserida, coloque o operador em uma nova linha e aumente a indentação antes dele.

```cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** Você pode usar espaços para alinhamento dentro de uma linha, se quiser.

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** Não use espaços ao redor dos operadores `.`, `->`.

Se necessário, o operador pode ser quebrado para a próxima linha. Nesse caso, o recuo à frente dele é aumentado.

**11.** Não use espaço para separar operadores unários (`--`, `++`, `*`, `&`, ...) do argumento.

**12.** Coloque um espaço após uma vírgula, mas não antes dela. A mesma regra vale para o ponto e vírgula dentro de uma expressão `for`.

**13.** Não use espaços para separar o operador `[]`.

**14.** Em uma expressão `template <...>`, use um espaço entre `template` e `<`; sem espaços após `<` ou antes de `>`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** Em classes e estruturas, escreva `public`, `private` e `protected` no mesmo nível de `class/struct`, e indente o restante do código.

```cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** Se o mesmo `namespace` for usado em todo o arquivo e não houver mais nada significativo, não é necessário usar recuo dentro do `namespace`.

**17.** Se o bloco de um `if`, `for`, `while` ou outra expressão consistir em um único `statement`, as chaves são opcionais. Em vez disso, coloque o `statement` em uma linha separada. Esta regra também é válida para `if`, `for`, `while` aninhados, ...

Mas se o `statement` interno contiver chaves ou `else`, o bloco externo deverá ser escrito entre chaves.

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** Não deve haver espaços ao final das linhas.

**19.** Os arquivos de código-fonte são codificados em UTF-8.

**20.** Caracteres não ASCII podem ser usados em literais de string.

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** Não escreva várias expressões em uma mesma linha.

**22.** Agrupe trechos de código dentro de funções e separe-os com no máximo uma linha em branco.

**23.** Separe funções, classes e afins com uma ou duas linhas em branco.

**24.** `A const` (quando se referir a um valor) deve ser escrito antes do nome do tipo.

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** Ao declarar um ponteiro ou uma referência, os símbolos `*` e `&` devem ser separados por espaços dos dois lados.

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Ao usar tipos template, defina aliases para eles com a palavra-chave `using` (exceto nos casos mais simples).

Em outras palavras, os parâmetros do template são especificados apenas em `using` e não se repetem no código.

`using` pode ser declarado localmente, por exemplo, dentro de uma função.

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** Não declare várias variáveis de tipos diferentes em uma mesma declaração.

```cpp
//incorrect
int x, *y;
```

**28.** Não use casts no estilo de C.

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** Em classes e structs, agrupe membros e funções separadamente dentro de cada escopo de visibilidade.

**30.** Para classes e structs pequenas, não é necessário separar a declaração do método da implementação.

O mesmo vale para métodos pequenos em quaisquer classes ou structs.

Para classes e structs template, não separe as declarações dos métodos da implementação (porque, caso contrário, eles terão de ser definidos na mesma unidade de tradução).

**31.** Você pode quebrar linhas em 140 caracteres, em vez de 80.

**32.** Sempre use os operadores de incremento/decremento pré-fixados se os pós-fixados não forem necessários.

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

<div id="comments">
  ## Comentários
</div>

**1.** Certifique-se de adicionar comentários a todo trecho de código que não seja trivial.

Isso é muito importante. Escrever o comentário pode ajudar você a perceber que o código não é necessário ou que foi concebido de forma inadequada.

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Os comentários podem ser tão detalhados quanto for necessário.

**3.** Coloque os comentários antes do código que eles descrevem. Em casos raros, os comentários podem vir depois do código, na mesma linha.

```cpp
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

**4.** Os comentários devem ser escritos somente em inglês.

**5.** Se você estiver escrevendo uma biblioteca, inclua comentários detalhados explicando seu funcionamento no arquivo de cabeçalho principal.

**6.** Não adicione comentários que não forneçam informações adicionais. Em particular, não deixe comentários vazios como este:

```cpp
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

O exemplo foi extraído da página http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/.

**7.** Não escreva comentários inúteis (autor, data de criação etc.) no início de cada arquivo.

**8.** Comentários de uma linha começam com três barras: `///` e comentários de várias linhas começam com `/**`. Esses comentários são considerados &quot;documentação&quot;.

Nota: Você pode usar o Doxygen para gerar documentação a partir desses comentários. Mas, em geral, o Doxygen não é usado, porque é mais prático navegar pelo código na IDE.

**9.** Comentários de várias linhas não devem ter linhas em branco no início nem no fim (exceto a linha que fecha um comentário de várias linhas).

**10.** Para comentar código, use comentários básicos, não comentários de &quot;documentação&quot;.

**11.** Exclua as partes comentadas do código antes de fazer commit.

**12.** Não use palavrões em comentários ou no código.

**13.** Não use letras maiúsculas. Não use pontuação excessiva.

```cpp
/// WHAT THE FAIL???
```

**14.** Não use comentários como delimitadores.

```cpp
///******************************************************
```

**15.** Não comece discussões nos comentários.

```cpp
/// Why did you do this stuff?
```

**16.** Não é necessário escrever um comentário no final de um bloco descrevendo do que ele trata.

```cpp
/// for
```

<div id="names">
  ## Nomes
</div>

**1.** Use letras minúsculas com sublinhados nos nomes de variáveis e membros de classe.

```cpp
size_t max_block_size;
```

**2.** Para nomes de funções (métodos), use camelCase começando com letra minúscula.

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** Para nomes de classes (structs), use CamelCase com inicial maiúscula. Prefixos diferentes de I não são usados para interfaces.

```cpp
class StorageMemory : public IStorage
```

**4.** `using` recebem nomes da mesma forma que as classes.

**5.** Nomes de argumentos de tipo de Template: em casos simples, use `T`; `T`, `U`; `T1`, `T2`.

Em casos mais complexos, siga as regras para nomes de classes ou adicione o prefixo `T`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Nomes de argumentos constantes de template: devem seguir as regras para nomes de variáveis ou, em casos simples, usar `N`.

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** Para classes abstratas (interfaces), você pode adicionar o prefixo `I`.

```cpp
class IProcessor
```

**8.** Se você usar uma variável local, pode usar um nome curto.

Em todos os outros casos, use um nome que descreva seu propósito.

```cpp
bool info_successfully_loaded = false;
```

**9.** Os nomes de `define`s e de constantes globais são escritos em ALL&#95;CAPS com sublinhados.

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** Os nomes de arquivos devem seguir o mesmo estilo do seu conteúdo.

Se um arquivo contiver uma única classe, nomeie o arquivo da mesma forma que a classe (CamelCase).

Se o arquivo contiver uma única função, nomeie o arquivo da mesma forma que a função (camelCase).

**11.** Se o nome contiver uma abreviação, então:

* Para nomes de variáveis, a abreviação deve usar letras minúsculas: `mysql_connection` (não `mySQL_connection`).
* Para nomes de classes e funções, mantenha as letras maiúsculas na abreviação: `MySQLConnection` (não `MySqlConnection`).

**12.** Os argumentos do construtor usados apenas para inicializar os membros da classe devem ser nomeados da mesma forma que os membros da classe, mas com um sublinhado no final.

```cpp
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

O sufixo de sublinhado pode ser omitido se o argumento não for usado no corpo do construtor.

**13.** Não há diferença entre os nomes das variáveis locais e dos membros da classe (não são necessários prefixos).

```cpp
timer (not m_timer)
```

**14.** Para as constantes de um `enum`, use CamelCase com inicial maiúscula. ALL&#95;CAPS também é aceitável. Se o `enum` não for local, use uma `enum class`.

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** Todos os nomes devem estar em inglês. Não é permitida a transliteração de palavras em hebraico.

não T&#95;PAAMAYIM&#95;NEKUDOTAYIM

**16.** Abreviações são aceitáveis se forem bem conhecidas (quando for fácil encontrar o significado da abreviação na Wikipedia ou em um mecanismo de busca).

`AST`, `SQL`.

Não `NVDH` (algumas letras aleatórias)

Palavras abreviadas são aceitáveis se a forma abreviada for de uso comum.

Você também pode usar uma abreviação se o nome completo estiver incluído ao lado dela nos comentários.

**17.** Nomes de arquivos com código-fonte C++ devem ter a extensão `.cpp`. Arquivos de cabeçalho devem ter a extensão `.h`.

<div id="how-to-write-code">
  ## Como escrever código
</div>

**1.** Gerenciamento de memória.

A desalocação manual de memória (`delete`) só pode ser usada em código de biblioteca.

Em código de biblioteca, o operador `delete` só pode ser usado em destrutores.

No código da aplicação, a memória deve ser liberada pelo objeto que é seu proprietário.

Exemplos:

* A forma mais fácil é colocar um objeto na pilha ou torná-lo membro de outra classe.
* Para um grande número de objetos pequenos, use contêineres.
* Para a desalocação automática de um pequeno número de objetos que residem no heap, use `shared_ptr/unique_ptr`.

**2.** Gerenciamento de recursos.

Use `RAII` e veja acima.

**3.** Tratamento de erros.

Use exceções. Na maioria dos casos, você só precisa lançar uma exceção e não precisa capturá-la (por causa de `RAII`).

Em aplicações de processamento de dados offline, muitas vezes é aceitável não capturar exceções.

Em servidores que lidam com requisições de usuários, geralmente basta capturar exceções no nível mais alto do handler de conexão.

Em funções de thread, você deve capturar e armazenar todas as exceções para relançá-las na thread principal após `join`.

```cpp
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

Nunca oculte exceções sem tratá-las. Nunca simplesmente registre cegamente todas as exceções no log.

```cpp
//Not correct
catch (...) {}
```

Se você precisar ignorar algumas exceções, faça isso apenas para exceções específicas e relance as demais.

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

Ao usar funções que retornam códigos de resposta ou `errno`, sempre verifique o resultado e lance uma exceção em caso de erro.

```cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

Você pode usar assert para verificar invariantes no código.

**4.** Tipos de exceção.

Não há necessidade de usar uma hierarquia de exceções complexa no código da aplicação. O texto da exceção deve ser compreensível para um administrador de sistemas.

**5.** Lançando exceções em destrutores.

Isso não é recomendado, mas é permitido.

Use as seguintes opções:

* Crie uma função (`done()` ou `finalize()`) que faça antecipadamente todo o trabalho que possa levar a uma exceção. Se essa função tiver sido chamada, não deverá haver exceções no destrutor depois.
* Tarefas complexas demais (como enviar mensagens pela rede) podem ser colocadas em um método separado que o usuário da classe terá de chamar antes da destruição.
* Se houver uma exceção no destrutor, é melhor registrá-la do que ocultá-la (se o logger estiver disponível).
* Em aplicações simples, é aceitável contar com `std::terminate` (para casos de `noexcept` por padrão no C++11) para lidar com exceções.

**6.** Blocos de código anônimos.

Você pode criar um bloco de código separado dentro de uma única função para tornar certas variáveis locais, de modo que os destrutores sejam chamados ao sair do bloco.

```cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multithreading.

Em programas offline de processamento de dados:

* Tente obter o melhor desempenho possível em um único núcleo de CPU. Depois, você pode paralelizar seu código, se necessário.

Em aplicações de servidor:

* Use o pool de threads para processar requisições. Até agora, não tivemos nenhuma tarefa que exigisse troca de contexto em userspace.

Fork não é usado para paralelização.

**8.** Sincronização de threads.

Muitas vezes, é possível fazer com que threads diferentes usem posições de memória diferentes (melhor ainda: linhas de cache diferentes) e não usar nenhuma sincronização entre threads (exceto `joinAll`).

Se a sincronização for necessária, na maioria dos casos, basta usar um mutex com `lock_guard`.

Nos outros casos, use primitivas de sincronização do sistema. Não use espera ocupada.

Operações atômicas devem ser usadas apenas nos casos mais simples.

Não tente implementar estruturas de dados sem bloqueio, a menos que essa seja sua principal área de especialização.

**9.** Ponteiros vs. referências.

Na maioria dos casos, prefira referências.

**10.** `const`.

Use referências constantes, ponteiros para constantes, `const_iterator` e métodos `const`.

Considere `const` como o padrão e use não-`const` apenas quando necessário.

Ao passar variáveis por valor, usar `const` geralmente não faz sentido.

**11.** unsigned.

Use `unsigned`, se necessário.

**12.** Tipos numéricos.

Use os tipos `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32` e `Int64`, bem como `size_t`, `ssize_t` e `ptrdiff_t`.

Não use estes tipos para números: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** Passagem de argumentos.

Passe valores complexos por valor se eles forem ser movidos, usando `std::move`; passe por referência se quiser atualizar o valor em um loop.

Se uma função assumir a posse de um objeto criado no heap, faça com que o tipo do argumento seja `shared_ptr` ou `unique_ptr`.

**14.** Valores de retorno.

Na maioria dos casos, basta usar `return`. Não escreva `return std::move(res)`.

Se a função alocar um objeto no heap e retorná-lo, use `shared_ptr` ou `unique_ptr`.

Em casos raros (ao atualizar um valor em um loop), talvez seja necessário retornar o valor por meio de um argumento. Nesse caso, o argumento deve ser uma referência.

```cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** `namespace`.

Não é necessário usar um `namespace` separado para o código da aplicação.

Bibliotecas pequenas também não precisam disso.

Para bibliotecas de médio a grande porte, coloque tudo em um `namespace`.

No arquivo `.h` da biblioteca, você pode usar `namespace detail` para ocultar detalhes de implementação que não são necessários para o código da aplicação.

Em um arquivo `.cpp`, você pode usar `static` ou um `namespace` anônimo para ocultar símbolos.

Além disso, um `namespace` pode ser usado com um `enum` para evitar que os nomes correspondentes vazem para um `namespace` externo (mas é melhor usar um `enum class`).

**16.** Inicialização adiada.

Se forem necessários argumentos para a inicialização, normalmente você não deve definir um construtor padrão.

Se mais tarde precisar adiar a inicialização, você pode adicionar um construtor padrão que criará um objeto inválido. Ou, para um pequeno número de objetos, pode usar `shared_ptr/unique_ptr`.

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** Funções virtuais.

Se a classe não se destina a uso polimórfico, não é necessário tornar as funções virtuais. Isso também se aplica ao destrutor.

**18.** Codificações.

Use UTF-8 em toda parte. Use `std::string` e `char *`. Não use `std::wstring` nem `wchar_t`.

**19.** Logging.

Veja os exemplos em todo o código.

Antes de fazer commit, remova todo logging sem sentido e de depuração, assim como qualquer outro tipo de saída de depuração.

O logging em laços deve ser evitado, mesmo no nível Trace.

Os logs devem ser legíveis em qualquer nível de logging.

Em geral, o logging deve ser usado apenas no código da aplicação.

As mensagens de log devem ser escritas em inglês.

De preferência, o log deve ser compreensível para o administrador do sistema.

Não use palavrões no log.

Use codificação UTF-8 no log. Em casos raros, você pode usar caracteres não ASCII no log.

**20.** Entrada e saída.

Não use `iostreams` em laços internos críticos para o desempenho da aplicação (e nunca use `stringstream`).

Use a biblioteca `DB/IO` em vez disso.

**21.** Data e hora.

Veja a biblioteca `DateLUT`.

**22.** include.

Sempre use `#pragma once` em vez de include guards.

**23.** using.

`using namespace` não deve ser usado. Você pode usar `using` para algo específico. Mas mantenha isso local, dentro de uma classe ou função.

**24.** Não use `trailing return type` em funções, a menos que seja necessário.

```cpp
auto f() -> void
```

**25.** Declaração e inicialização de variáveis.

```cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** Para funções virtuais, escreva `virtual` na classe base, mas use `override` em vez de `virtual` nas classes derivadas.

<div id="unused-features-of-c">
  ## Recursos não utilizados do C++
</div>

**1.** A herança virtual não é utilizada.

**2.** Construtos que têm açúcar sintático prático no C++ moderno, por exemplo.

```cpp
// Traditional way without syntactic sugar
template <typename G, typename = std::enable_if_t<std::is_same<G, F>::value, void>> // SFINAE via std::enable_if, usage of ::value
std::pair<int, int> func(const E<G> & e) // explicitly specified return type
{
    if (elements.count(e)) // .count() membership test
    {
        // ...
    }

    elements.erase(
        std::remove_if(
            elements.begin(), elements.end(),
            [&](const auto x){
                return x == 1;
            }),
        elements.end()); // remove-erase idiom

    return std::make_pair(1, 2); // create pair via make_pair()
}

// With syntactic sugar (C++14/17/20)
template <typename G>
requires std::same_v<G, F> // SFINAE via C++20 concept, usage of C++14 template alias
auto func(const E<G> & e) // auto return type (C++14)
{
    if (elements.contains(e)) // C++20 .contains membership test
    {
        // ...
    }

    elements.erase_if(
        elements,
        [&](const auto x){
            return x == 1;
        }); // C++20 std::erase_if

    return {1, 2}; // or: return std::pair(1, 2); // create pair via initialization list or value initialization (C++17)
}
```

<div id="platform">
  ## Plataforma
</div>

**1.** Escrevemos código para uma plataforma específica.

Mas, em igualdade de condições, prefere-se código multiplataforma ou portável.

**2.** Linguagem: C++20 (veja a lista de [funcionalidades disponíveis do C++20](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features)).

**3.** Compilador: `clang`. No momento da redação deste texto (março de 2025), o código é compilado com clang versão &gt;= 19.

A biblioteca padrão é usada (`libc++`).

**4.** SO: Ubuntu Linux, não anterior ao Precise.

**5.** O código é escrito para a arquitetura de CPU x86&#95;64.

O conjunto de instruções da CPU é o conjunto mínimo com suporte em nossos servidores. Atualmente, é SSE 4.2.

**6.** Use as flags de compilação `-Wall -Wextra -Werror -Weverything` com algumas exceções.

**7.** Use linkagem estática com todas as bibliotecas, exceto aquelas que são difíceis de linkar estaticamente (veja a saída do comando `ldd`).

**8.** O código é desenvolvido e depurado com configurações de release.

<div id="tools">
  ## Ferramentas
</div>

**1.** O KDevelop é uma boa IDE.

**2.** Para depuração, use `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...` ou `tcmalloc_minimal_debug`.

**3.** Para análise de desempenho, use `Linux Perf`, `valgrind` (`callgrind`) ou `strace -cf`.

**4.** O código-fonte está no Git.

**5.** A compilação usa `CMake`.

**6.** Os programas são distribuídos em pacotes `deb`.

**7.** Os commits na master não devem quebrar a compilação.

No entanto, apenas revisões selecionadas são consideradas utilizáveis.

**8.** Faça commits com a maior frequência possível, mesmo que o código esteja apenas parcialmente pronto.

Use branches para isso.

Se o seu código na branch `master` ainda não puder ser compilado, exclua-o da compilação antes do `push`. Você precisará finalizá-lo ou removê-lo em alguns dias.

**9.** Para mudanças não triviais, use branches e publique-as no servidor.

**10.** Código não utilizado é removido do repositório.

<div id="libraries">
  ## Bibliotecas
</div>

**1.** A biblioteca padrão do C++20 é usada (extensões experimentais são permitidas), assim como os frameworks `boost` e `Poco`.

**2.** Não é permitido usar bibliotecas de pacotes do SO. Também não é permitido usar bibliotecas pré-instaladas. Todas as bibliotecas devem ser incluídas na forma de código-fonte no diretório `contrib` e compiladas junto com o ClickHouse. Veja [Diretrizes para adicionar novas bibliotecas de terceiros](/pt-BR/development/contrib#adding-and-maintaining-third-party-libraries) para mais detalhes.

**3.** Deve-se sempre dar preferência a bibliotecas que já estão em uso.

<div id="general-recommendations">
  ## Recomendações gerais
</div>

**1.** Escreva o mínimo de código possível.

**2.** Tente a solução mais simples.

**3.** Não escreva código até saber como ele vai funcionar e como o laço interno vai funcionar.

**4.** Nos casos mais simples, use `using` em vez de classes ou structs.

**5.** Se possível, não escreva construtores de cópia, operadores de atribuição, destrutores (exceto um destrutor virtual, se a classe contiver pelo menos uma função virtual), construtores de movimento nem operadores de atribuição por movimento. Em outras palavras, as funções geradas pelo compilador devem funcionar corretamente. Você pode usar `default`.

**6.** Prefira simplificar o código. Reduza-o sempre que possível.

<div id="additional-recommendations">
  ## Recomendações adicionais
</div>

**1.** Especificar explicitamente `std::` para tipos de `stddef.h`

não é recomendado. Em outras palavras, recomendamos escrever `size_t` em vez de `std::size_t`, porque é mais curto.

É aceitável adicionar `std::`.

**2.** Especificar explicitamente `std::` para funções da biblioteca padrão de C

não é recomendado. Em outras palavras, escreva `memcpy` em vez de `std::memcpy`.

O motivo é que existem funções não padronizadas semelhantes, como `memmem`. Nós as usamos ocasionalmente. Essas funções não existem no `namespace std`.

Se você escrever `std::memcpy` em vez de `memcpy` em toda parte, `memmem` sem `std::` vai parecer estranho.

Ainda assim, você pode usar `std::` se preferir.

**3.** Usar funções de C quando as mesmas também estiverem disponíveis na biblioteca padrão de C++.

Isso é aceitável se for mais eficiente.

Por exemplo, use `memcpy` em vez de `std::copy` para copiar grandes blocos de memória.

**4.** Argumentos de função em várias linhas.

Qualquer um dos estilos de quebra de linha a seguir é permitido:

```cpp
function(
  T1 x1,
  T2 x2)
```

```cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

```cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

```cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

```cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```