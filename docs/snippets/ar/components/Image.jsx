export const Image = ({ img, alt, size = "lg", background }) => {
  const normalizedSize = ["sm", "md", "lg"].includes(size) ? size : "lg";
  const backgroundColor = background === "white"
    ? "white"
    : background === "black"
      ? "rgb(31 31 28)"
      : undefined;

  return (
    <div className={`ch-image-${normalizedSize}`}>
      <Frame>
        <img src={img} alt={alt} style={{ backgroundColor }} />
      </Frame>
    </div>
  );
};