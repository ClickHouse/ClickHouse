const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify admite estilos en línea y renderiza fielmente los children de MDX —
  // este sencillo envoltorio basta para que el contenido sea legible. La
  // experiencia de usuario de la barra de desplazamiento superior sincronizada del Docusaurus original es un extra que podemos
  // omitir sin que el contenido se vea afectado.
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;