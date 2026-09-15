export const Image = ({ img, alt, size = "lg" }) => {
  const normalizedSize = ["sm", "md", "lg"].includes(size) ? size : "lg";

  return (
    <div className={`ch-image-${normalizedSize}`}>
      <Frame>
        <img src={img} alt={alt} />
      </Frame>
    </div>
  );
};