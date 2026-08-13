const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify поддерживает инлайн-стили и корректно отображает дочерние элементы MDX —
  // для удобного чтения содержимого достаточно простого компонента-обёртки. Синхронизированная
  // верхняя полоса прокрутки, как в оригинале Docusaurus, — полезное, но необязательное улучшение,
  // поэтому без неё можно обойтись без ущерба для содержимого.
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;