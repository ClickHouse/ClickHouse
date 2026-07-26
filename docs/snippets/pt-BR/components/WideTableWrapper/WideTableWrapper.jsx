const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // O Mintlify oferece suporte a estilos inline e renderiza os elementos filhos de MDX com fidelidade —
  // esse wrapper simples já é suficiente para a legibilidade do conteúdo. A experiência de uso da
  // barra de rolagem superior sincronizada no Docusaurus original é algo desejável,
  // mas podemos dispensá-la sem comprometer o conteúdo.
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;